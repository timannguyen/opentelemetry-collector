// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package memorylimiter

import (
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestUnmarshalDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NoError(t, component.UnmarshalConfig(confmap.New(), cfg))
	assert.Equal(t, factory.CreateDefaultConfig(), cfg)
}

func TestUnmarshalConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NoError(t, component.UnmarshalConfig(cm, cfg))
	assert.Equal(t,
		&Config{
			CheckInterval:       5 * time.Second,
			MemoryLimitMiB:      4000,
			MemorySpikeLimitMiB: 500,
		}, cfg)
}

func TestValidateConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	// Valid Config
	assert.NoError(t, component.UnmarshalConfig(cm, cfg))
	assert.NoError(t, cfg.(*Config).Validate())
	// Invalid Interval
	pCfg := cfg.(*Config)
	pCfg.CheckInterval = 0
	assert.Error(t, pCfg.Validate(), errCheckIntervalOutOfRange)
	// Invalid mem limit
	pCfg = cfg.(*Config)
	pCfg.CheckInterval = 1
	pCfg.MemoryLimitMiB = 0
	pCfg.MemoryLimitPercentage = 0
	assert.Error(t, pCfg.Validate(), errLimitOutOfRange)
}

func TestMemoryLimitation(t *testing.T) {
	var mlc = &MemoryLimitation{
		MemoryLimiterID: component.NewID("ml"),
	}
	exts := make(map[component.ID]component.Component)
	t.Run("extension not found", func(t *testing.T) {
		e, err := mlc.GetMemoryLimiter(exts)
		require.Error(t, err)
		assert.Nil(t, e)
	})

	exts[component.NewID("ml")] = &memoryLimiter{
		usageChecker: memUsageChecker{
			memAllocLimit: 1024,
		},
		mustRefuse: &atomic.Bool{},
		readMemStatsFn: func(ms *runtime.MemStats) {
			ms.Alloc = 100
		},
		logger: zap.NewNop(),
	}
	t.Run("extension found", func(t *testing.T) {
		ml, err := mlc.GetMemoryLimiter(exts)
		assert.NoError(t, err)
		assert.NoError(t, ml.CheckMemory())
	})
}
