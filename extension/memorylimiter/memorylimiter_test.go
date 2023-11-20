// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package memorylimiter

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/internal/iruntime"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
)

func TestNew(t *testing.T) {
	type args struct {
		nextConsumer        consumer.Traces
		checkInterval       time.Duration
		memoryLimitMiB      uint32
		memorySpikeLimitMiB uint32
	}
	sink := new(consumertest.TracesSink)
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "zero_checkInterval",
			args: args{
				nextConsumer: sink,
			},
			wantErr: errCheckIntervalOutOfRange,
		},
		{
			name: "zero_memAllocLimit",
			args: args{
				nextConsumer:  sink,
				checkInterval: 100 * time.Millisecond,
			},
			wantErr: errLimitOutOfRange,
		},
		{
			name: "memSpikeLimit_gt_memAllocLimit",
			args: args{
				nextConsumer:        sink,
				checkInterval:       100 * time.Millisecond,
				memoryLimitMiB:      1,
				memorySpikeLimitMiB: 2,
			},
			wantErr: errMemSpikeLimitOutOfRange,
		},
		{
			name: "success",
			args: args{
				nextConsumer:   sink,
				checkInterval:  100 * time.Millisecond,
				memoryLimitMiB: 1024,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{}
			cfg.CheckInterval = tt.args.checkInterval
			cfg.MemoryLimitMiB = tt.args.memoryLimitMiB
			cfg.MemorySpikeLimitMiB = tt.args.memorySpikeLimitMiB
			got, err := newMemoryLimiter(cfg, zap.NewNop())
			ctx := context.Background()
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			assert.NoError(t, err)
			got.Start(ctx, nil)
			assert.NoError(t, got.Shutdown(ctx))
		})
	}
}

func TestMemoryPressureResponse(t *testing.T) {
	var currentMemAlloc uint64
	ml := &memoryLimiter{
		usageChecker: memUsageChecker{
			memAllocLimit: 1024,
		},
		mustRefuse: &atomic.Bool{},
		readMemStatsFn: func(ms *runtime.MemStats) {
			ms.Alloc = currentMemAlloc
		},
		logger: zap.NewNop(),
	}

	// Below memAllocLimit.
	currentMemAlloc = 800
	ml.checkMemLimits()
	assert.NoError(t, ml.CheckMemory())

	// Above memAllocLimit.
	currentMemAlloc = 1800
	ml.checkMemLimits()
	assert.Equal(t, errDataRefused, ml.CheckMemory())

	// Check spike limit
	ml.usageChecker.memSpikeLimit = 512

	// Below memSpikeLimit.
	currentMemAlloc = 500
	ml.checkMemLimits()
	assert.NoError(t, ml.CheckMemory())

	// Above memSpikeLimit.
	currentMemAlloc = 550
	ml.checkMemLimits()
	assert.Equal(t, errDataRefused, ml.CheckMemory())
}

func TestGetDecision(t *testing.T) {
	t.Run("fixed_limit", func(t *testing.T) {
		d, err := getMemUsageChecker(&Config{MemoryLimitMiB: 100, MemorySpikeLimitMiB: 20}, zap.NewNop())
		require.NoError(t, err)
		assert.Equal(t, &memUsageChecker{
			memAllocLimit: 100 * mibBytes,
			memSpikeLimit: 20 * mibBytes,
		}, d)
	})
	t.Run("fixed_limit_error", func(t *testing.T) {
		d, err := getMemUsageChecker(&Config{MemoryLimitMiB: 20, MemorySpikeLimitMiB: 100}, zap.NewNop())
		require.Error(t, err)
		assert.Nil(t, d)
	})

	t.Cleanup(func() {
		getMemoryFn = iruntime.TotalMemory
	})
	getMemoryFn = func() (uint64, error) {
		return 100 * mibBytes, nil
	}
	t.Run("percentage_limit", func(t *testing.T) {
		d, err := getMemUsageChecker(&Config{MemoryLimitPercentage: 50, MemorySpikePercentage: 10}, zap.NewNop())
		require.NoError(t, err)
		assert.Equal(t, &memUsageChecker{
			memAllocLimit: 50 * mibBytes,
			memSpikeLimit: 10 * mibBytes,
		}, d)
	})
	t.Run("percentage_limit_error", func(t *testing.T) {
		d, err := getMemUsageChecker(&Config{MemoryLimitPercentage: 101, MemorySpikePercentage: 10}, zap.NewNop())
		require.Error(t, err)
		assert.Nil(t, d)
		d, err = getMemUsageChecker(&Config{MemoryLimitPercentage: 99, MemorySpikePercentage: 101}, zap.NewNop())
		require.Error(t, err)
		assert.Nil(t, d)
	})
}

func TestRefuseDecision(t *testing.T) {
	decison1000Limit30Spike30, err := newPercentageMemUsageChecker(1000, 60, 30)
	require.NoError(t, err)
	decison1000Limit60Spike50, err := newPercentageMemUsageChecker(1000, 60, 50)
	require.NoError(t, err)
	decison1000Limit40Spike20, err := newPercentageMemUsageChecker(1000, 40, 20)
	require.NoError(t, err)
	decison1000Limit40Spike60, err := newPercentageMemUsageChecker(1000, 40, 60)
	require.Error(t, err)
	assert.Nil(t, decison1000Limit40Spike60)

	tests := []struct {
		name         string
		usageChecker memUsageChecker
		ms           *runtime.MemStats
		shouldRefuse bool
	}{
		{
			name:         "should refuse over limit",
			usageChecker: *decison1000Limit30Spike30,
			ms:           &runtime.MemStats{Alloc: 600},
			shouldRefuse: true,
		},
		{
			name:         "should not refuse",
			usageChecker: *decison1000Limit30Spike30,
			ms:           &runtime.MemStats{Alloc: 100},
			shouldRefuse: false,
		},
		{
			name: "should not refuse spike, fixed usageChecker",
			usageChecker: memUsageChecker{
				memAllocLimit: 600,
				memSpikeLimit: 500,
			},
			ms:           &runtime.MemStats{Alloc: 300},
			shouldRefuse: true,
		},
		{
			name:         "should refuse, spike, percentage usageChecker",
			usageChecker: *decison1000Limit60Spike50,
			ms:           &runtime.MemStats{Alloc: 300},
			shouldRefuse: true,
		},
		{
			name:         "should refuse, spike, percentage usageChecker",
			usageChecker: *decison1000Limit40Spike20,
			ms:           &runtime.MemStats{Alloc: 250},
			shouldRefuse: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			shouldRefuse := test.usageChecker.aboveSoftLimit(test.ms)
			assert.Equal(t, test.shouldRefuse, shouldRefuse)
		})
	}
}
