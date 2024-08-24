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
	Workdir              string
	WriteAheadLogging    WriteAheadLogging
	Processors           []ProcessorConfig
	IgnoreRecoveryErrors bool
	MaxWorkers           int
}

type ProcessorConfig struct {
	Processor definitions.Processor
	Retry     ProcessorRetryMechanism
}

type ProcessorRetryMechanism struct {
	MaxRetries      int
	BackOffInterval time.Duration
}
