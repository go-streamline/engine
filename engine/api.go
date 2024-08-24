package engine

import (
	"context"
	"github.com/alitto/pond"
	"github.com/go-streamline/core/config"
	"github.com/go-streamline/core/definitions"
	"github.com/go-streamline/core/errors"
	"github.com/go-streamline/core/repo"
	"github.com/go-streamline/core/utils"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"io"
)

func New(ctx context.Context, config *config.Config, writeAheadLogger repo.WriteAheadLogger, log *logrus.Logger) (*Engine, error) {
	err := utils.CreateDirsIfNotExist(config.Workdir)
	if err != nil {
		return nil, errors.CouldNotCreateDirs
	}

	config, err = DeepCopier.DeepCopyConfig(config)
	if err != nil {
		return nil, errors.CouldNotDeepCopyConfig
	}

	return &Engine{
		config:                config,
		Processors:            config.Processors,
		ctx:                   ctx,
		incomingQueue:         make(chan definitions.EngineIncomingObject),
		sessionUpdatesChannel: make(chan definitions.SessionUpdate),
		writeAheadLogger:      writeAheadLogger,
		ignoreRecoveryErrors:  config.IgnoreRecoveryErrors,
		workerPool:            pond.New(config.MaxWorkers, config.MaxWorkers),
		log:                   log,
		retryQueue:            make(chan retryTask, config.MaxWorkers),
	}, nil
}

func NewWithDefaults(ctx context.Context, writeAheadLogger repo.WriteAheadLogger, log *logrus.Logger, processors []config.ProcessorConfig) (*Engine, error) {
	return New(
		ctx,
		&config.Config{
			MaxWorkers:           10,
			Workdir:              "/tmp/go-streamline",
			Processors:           processors,
			IgnoreRecoveryErrors: false,
		}, writeAheadLogger, log)
}

func (e *Engine) Submit(metadata map[string]interface{}, reader io.Reader) uuid.UUID {
	sessionID := uuid.New()
	e.incomingQueue <- definitions.EngineIncomingObject{
		Metadata:  metadata,
		Reader:    reader,
		SessionID: sessionID,
	}
	return sessionID
}

func (e *Engine) SessionUpdates() <-chan definitions.SessionUpdate {
	return e.sessionUpdatesChannel
}

func (e *Engine) Run() error {
	err := e.recover()
	if err != nil && !e.ignoreRecoveryErrors {
		return errors.RecoveryError
	}
	go func() {
		e.handleFiles()
	}()
	return nil
}
