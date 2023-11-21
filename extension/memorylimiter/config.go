// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package memorylimiter provides an extension for OpenTelemetry Service
// that refuses data according to the current state of memory usage.
package memorylimiter // import "go.opentelemetry.io/collector/extension/memorylimiter"

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
)

// Config defines configuration for memory memoryLimiter extension.
type Config struct {
	// CheckInterval is the time between measurements of memory usage for the
	// purposes of avoiding going over the limits. Defaults to zero, so no
	// checks will be performed.
	CheckInterval time.Duration `mapstructure:"check_interval"`

	// MemoryLimitMiB is the maximum amount of memory, in MiB, targeted to be
	// allocated by the extension.
	MemoryLimitMiB uint32 `mapstructure:"limit_mib"`

	// MemorySpikeLimitMiB is the maximum, in MiB, spike expected between the
	// measurements of memory usage.
	MemorySpikeLimitMiB uint32 `mapstructure:"spike_limit_mib"`

	// MemoryLimitPercentage is the maximum amount of memory, in %, targeted to be
	// allocated by the extension. The fixed memory settings MemoryLimitMiB has a higher precedence.
	MemoryLimitPercentage uint32 `mapstructure:"limit_percentage"`

	// MemorySpikePercentage is the maximum, in percents against the total memory,
	// spike expected between the measurements of memory usage.
	MemorySpikePercentage uint32 `mapstructure:"spike_limit_percentage"`
}

// MemoryLimitation defines memory limiter path for the receiver
type MemoryLimitation struct {
	// MemoryLimiterID specifies the name of the memory limiter extension
	MemoryLimiterID component.ID `mapstructure:"memory_limiter"`
}

// GetMemoryLimiter attempts to find a memory limiter extension in the extension list.
// If a memory limiter extension is not found, an error is returned.
func (m *MemoryLimitation) GetMemoryLimiter(extensions map[component.ID]component.Component) (MemoryLimiter, error) {
	if ext, found := extensions[m.MemoryLimiterID]; found {
		if ml, ok := ext.(MemoryLimiter); ok {
			return ml, nil
		}
	}
	return nil, fmt.Errorf("failed to resolve Memory Limiter %q", m.MemoryLimiterID)
}

var _ component.Config = (*Config)(nil)

// Validate checks if the extension configuration is valid
func (cfg *Config) Validate() error {
	if cfg.CheckInterval <= 0 {
		return errCheckIntervalOutOfRange
	}

	if cfg.MemoryLimitMiB == 0 && cfg.MemoryLimitPercentage == 0 {
		return errLimitOutOfRange
	}

	return nil
}
