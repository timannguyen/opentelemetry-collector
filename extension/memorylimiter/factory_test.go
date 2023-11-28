// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package memorylimiter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/extension/extensiontest"

	"go.opentelemetry.io/collector/component/componenttest"
)

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	require.NotNil(t, factory)

	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
}

func TestCreateExtension(t *testing.T) {
	factory := NewFactory()
	require.NotNil(t, factory)

	cfg := factory.CreateDefaultConfig()
	// This extension can't be created with the default config.
	tp, err := factory.CreateExtension(context.Background(), extensiontest.NewNopCreateSettings(), cfg)
	assert.Nil(t, tp)
	assert.Error(t, err, "created extension with invalid settings")

	// Create extension with a valid config.
	pCfg := cfg.(*Config)
	pCfg.MemoryLimitMiB = 5722
	pCfg.MemorySpikeLimitMiB = 1907
	pCfg.CheckInterval = 100 * time.Millisecond

	tp, err = factory.CreateExtension(context.Background(), extensiontest.NewNopCreateSettings(), cfg)
	assert.NoError(t, err)
	assert.NotNil(t, tp)
	// test if we can shutdown a monitoring routine that has not started
	assert.ErrorIs(t, tp.Shutdown(context.Background()), errShutdownNotStarted)
	assert.NoError(t, tp.Start(context.Background(), componenttest.NewNopHost()))

	assert.NoError(t, tp.Shutdown(context.Background()))
	// verify that no monitoring routine is running
	assert.Error(t, tp.Shutdown(context.Background()))
}
