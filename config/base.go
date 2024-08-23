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
	Handlers             []HandlerConfig
	IgnoreRecoveryErrors bool
	MaxWorkers           int
}

type HandlerConfig struct {
	Handler definitions.Handler
	Retry   HandlerRetryMechanism
}

type HandlerRetryMechanism struct {
	MaxRetries      int
	BackOffInterval time.Duration
}
