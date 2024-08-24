package config

import (
	"github.com/go-streamline/core/definitions"
	"time"
)

type WriteAheadLogging struct {
	MaxSizeMB  int
	MaxBackups int
	MaxAgeDays int
	Enabled    bool
}

type Config struct {
	Workdir              string            `json:"workdir"`
	Processors           []ProcessorConfig `json:"processors"`
	IgnoreRecoveryErrors bool              `json:"ignoreRecoveryErrors"`
	MaxWorkers           int               `json:"maxWorkers"`
	InitRetryBackOff     time.Duration     `json:"initRetryBackOff"`
}

type ProcessorConfig struct {
	Processor definitions.Processor
	Retry     ProcessorRetryMechanism
}

type ProcessorRetryMechanism struct {
	MaxRetries      int
	BackOffInterval time.Duration
}
