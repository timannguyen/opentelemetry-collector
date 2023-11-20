// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package memorylimiter

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
