// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package memorylimiter // import "go.opentelemetry.io/collector/extension/memorylimiter"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

const (
	// The value of "type" Attribute Key in configuration.
	typeStr = "memory_limiter"
)

// NewFactory returns a new factory for the Memory Limiter extension.
func NewFactory() extension.Factory {
	return extension.NewFactory(
		typeStr,
		createDefaultConfig,
		createExtension,
		component.StabilityLevelAlpha)
}

// CreateDefaultConfig creates the default configuration for extension. Notice
// that the default configuration is expected to fail for this extension.
func createDefaultConfig() component.Config {
	return &Config{}
}

func createExtension(_ context.Context, set extension.CreateSettings, cfg component.Config) (extension.Extension, error) {
	return newMemoryLimiter(cfg.(*Config), set.TelemetrySettings.Logger)
}
