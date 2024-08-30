package config

import (
	"github.com/go-streamline/interfaces/definitions"
	"time"
)

type Config struct {
	Workdir          string            `json:"workdir"`
	Processors       []ProcessorConfig `json:"processors"`
	MaxWorkers       int               `json:"maxWorkers"`
	InitRetryBackOff time.Duration     `json:"initRetryBackOff"`
}

type ProcessorConfig struct {
	Processor definitions.Processor
	Retry     ProcessorRetryMechanism
}

type ProcessorRetryMechanism struct {
	MaxRetries      int
	BackOffInterval time.Duration
}
