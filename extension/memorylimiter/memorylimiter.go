// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package memorylimiter // import "go.opentelemetry.io/collector/extension/memorylimiter"

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/internal/iruntime"
)

const (
	mibBytes = 1024 * 1024

	// Minimum interval between forced GC when in soft limited mode. We don't want to
	// do GCs too frequently since it is a CPU-heavy operation.
	minGCIntervalWhenSoftLimited = 10 * time.Second
)

var (
	// errDataRefused will be returned to callers of ConsumeTraceData to indicate
	// that data is being refused due to high memory usage.
	errDataRefused = errors.New("data refused due to high memory usage")

	// Construction errors

	errCheckIntervalOutOfRange = errors.New(
		"checkInterval must be greater than zero")

	errLimitOutOfRange = errors.New(
		"memAllocLimit or memoryLimitPercentage must be greater than zero")

	errMemSpikeLimitOutOfRange = errors.New(
		"memSpikeLimit must be smaller than memAllocLimit")

	errPercentageLimitOutOfRange = errors.New(
		"memoryLimitPercentage and memorySpikePercentage must be greater than zero and less than or equal to hundred",
	)

	errShutdownNotStarted = errors.New("no existing monitoring routine is running")

	// make it overridable by tests
	getMemoryFn = iruntime.TotalMemory
)

// MemoryLimiter is an Extension control memory usage of OpenTelemetry Collector.
// Common usage is to check memory usage during connection and can reject or accept the connection based on the
// configured memoryLimiter settings
type MemoryLimiter interface {
	// CheckMemory check if memory usage is above soft limits and return error to refuse data if is above.
	CheckMemory() error
}

type memoryLimiter struct {
	usageChecker memUsageChecker

	memCheckWait time.Duration

	// mustRefuse is used to indicate when data should be refused.
	mustRefuse *atomic.Bool

	ticker     *time.Ticker
	lastGCDone time.Time

	// The function to read the mem values is set as a reference to help with
	// testing different values.
	readMemStatsFn func(m *runtime.MemStats)

	// Fields used for logging.
	logger *zap.Logger

	refCounterLock sync.Mutex
	refCounter     int
}

// newMemoryLimiter returns a new memorylimiter extension.
func newMemoryLimiter(cfg *Config, logger *zap.Logger) (*memoryLimiter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	usageChecker, err := getMemUsageChecker(cfg, logger)
	if err != nil {
		return nil, err
	}

	logger.Info("Memory limiter configured",
		zap.Uint64("limit_mib", usageChecker.memAllocLimit/mibBytes),
		zap.Uint64("spike_limit_mib", usageChecker.memSpikeLimit/mibBytes),
		zap.Duration("check_interval", cfg.CheckInterval))

	ml := &memoryLimiter{
		usageChecker:   *usageChecker,
		memCheckWait:   cfg.CheckInterval,
		ticker:         time.NewTicker(cfg.CheckInterval),
		readMemStatsFn: runtime.ReadMemStats,
		logger:         logger,
		mustRefuse:     &atomic.Bool{},
	}

	return ml, nil
}

func getMemUsageChecker(cfg *Config, logger *zap.Logger) (*memUsageChecker, error) {
	memAllocLimit := uint64(cfg.MemoryLimitMiB) * mibBytes
	memSpikeLimit := uint64(cfg.MemorySpikeLimitMiB) * mibBytes
	if cfg.MemoryLimitMiB != 0 {
		return newFixedMemUsageChecker(memAllocLimit, memSpikeLimit)
	}

	totalMemory, err := getMemoryFn()
	if err != nil {
		return nil, fmt.Errorf("failed to get total memory, use fixed memory settings (limit_mib): %w", err)
	}
	logger.Info("Using percentage memory limiter",
		zap.Uint64("total_memory_mib", totalMemory/mibBytes),
		zap.Uint32("limit_percentage", cfg.MemoryLimitPercentage),
		zap.Uint32("spike_limit_percentage", cfg.MemorySpikePercentage))

	return newPercentageMemUsageChecker(totalMemory, uint64(cfg.MemoryLimitPercentage), uint64(cfg.MemorySpikePercentage))
}

func (ml *memoryLimiter) Start(_ context.Context, _ component.Host) error {
	ml.startMonitoring()
	return nil
}

func (ml *memoryLimiter) Shutdown(_ context.Context) error {
	ml.refCounterLock.Lock()
	defer ml.refCounterLock.Unlock()

	if ml.refCounter == 0 {
		return errShutdownNotStarted
	} else if ml.refCounter == 1 {
		ml.ticker.Stop()
	}
	ml.refCounter--
	return nil
}

// CheckMemory check if memory usage is above soft limits and return error to refuse data if is above.
func (ml *memoryLimiter) CheckMemory() error {
	if ml.mustRefuse.Load() {
		// TODO: actually to be 100% sure that this is "refused" and not "dropped"
		// 	it is necessary to check the pipeline to see if this is directly connected
		// 	to a receiver (ie.: a receiver is on the call stack). For now it
		// 	assumes that the pipeline is properly configured and a receiver is on the
		// 	callstack and that the receiver will correctly retry the refused data again.

		return errDataRefused
	}

	// Even if the next consumer returns error record the data as accepted by
	// this extension.
	return nil
}

func (ml *memoryLimiter) readMemStats() *runtime.MemStats {
	ms := &runtime.MemStats{}
	ml.readMemStatsFn(ms)
	return ms
}

// startMonitoring starts a single ticker'd goroutine per instance
// that will check memory usage every checkInterval period.
func (ml *memoryLimiter) startMonitoring() {
	ml.refCounterLock.Lock()
	defer ml.refCounterLock.Unlock()

	ml.refCounter++
	if ml.refCounter == 1 {
		go func() {
			for range ml.ticker.C {
				ml.checkMemLimits()
			}
		}()
	}
}

func memstatToZapField(ms *runtime.MemStats) zap.Field {
	return zap.Uint64("cur_mem_mib", ms.Alloc/mibBytes)
}

func (ml *memoryLimiter) doGCandReadMemStats() *runtime.MemStats {
	runtime.GC()
	ml.lastGCDone = time.Now()
	ms := ml.readMemStats()
	ml.logger.Debug("Memory usage after GC.", memstatToZapField(ms))
	return ms
}

func (ml *memoryLimiter) checkMemLimits() {
	ms := ml.readMemStats()

	ml.logger.Debug("Currently used memory.", memstatToZapField(ms))

	if ml.usageChecker.aboveHardLimit(ms) {
		ml.logger.Warn("Memory usage is above hard limit. Forcing a GC.", memstatToZapField(ms))
		ms = ml.doGCandReadMemStats()
	}

	// Remember current state.
	wasRefusing := ml.mustRefuse.Load()

	// Check if the memory usage is above the soft limit.
	mustRefuse := ml.usageChecker.aboveSoftLimit(ms)

	if wasRefusing && !mustRefuse {
		// Was previously refusing but enough memory is available now, no need to limit.
		ml.logger.Debug("Memory usage back within limits. Resuming normal operation.", memstatToZapField(ms))
	}

	if !wasRefusing && mustRefuse {
		// We are above soft limit, do a GC if it wasn't done recently and see if
		// it brings memory usage below the soft limit.
		if time.Since(ml.lastGCDone) > minGCIntervalWhenSoftLimited {
			ml.logger.Debug("Memory usage is above soft limit. Forcing a GC.", memstatToZapField(ms))
			ms = ml.doGCandReadMemStats()
			// Check the limit again to see if GC helped.
			mustRefuse = ml.usageChecker.aboveSoftLimit(ms)
		}

		if mustRefuse {
			ml.logger.Warn("Memory usage is above soft limit. Refusing data.", memstatToZapField(ms))
		}
	}

	ml.mustRefuse.Store(mustRefuse)
}

type memUsageChecker struct {
	memAllocLimit uint64
	memSpikeLimit uint64
}

func (d memUsageChecker) aboveSoftLimit(ms *runtime.MemStats) bool {
	return ms.Alloc >= d.memAllocLimit-d.memSpikeLimit
}

func (d memUsageChecker) aboveHardLimit(ms *runtime.MemStats) bool {
	return ms.Alloc >= d.memAllocLimit
}

func newFixedMemUsageChecker(memAllocLimit, memSpikeLimit uint64) (*memUsageChecker, error) {
	if memSpikeLimit >= memAllocLimit {
		return nil, errMemSpikeLimitOutOfRange
	}
	if memSpikeLimit == 0 {
		// If spike limit is unspecified use 20% of mem limit.
		memSpikeLimit = memAllocLimit / 5
	}
	return &memUsageChecker{
		memAllocLimit: memAllocLimit,
		memSpikeLimit: memSpikeLimit,
	}, nil
}

func newPercentageMemUsageChecker(totalMemory uint64, percentageLimit, percentageSpike uint64) (*memUsageChecker, error) {
	if percentageLimit > 100 || percentageLimit <= 0 || percentageSpike > 100 || percentageSpike <= 0 {
		return nil, errPercentageLimitOutOfRange
	}
	return newFixedMemUsageChecker(percentageLimit*totalMemory/100, percentageSpike*totalMemory/100)
}
