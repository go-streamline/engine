package engine

import (
	"context"
	"fmt"
	"github.com/alitto/pond"
	"github.com/go-streamline/core/config"
	"github.com/go-streamline/core/definitions"
	"github.com/go-streamline/core/errors"
	"github.com/go-streamline/core/repo"
	"github.com/go-streamline/core/utils"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"io"
)

func New(config *config.Config, writeAheadLogger repo.WriteAheadLogger, log *logrus.Logger, processorFactory definitions.ProcessorFactory, db *gorm.DB) (*Engine, error) {
	err := utils.CreateDirsIfNotExist(config.Workdir)
	if err != nil {
		return nil, errors.CouldNotCreateDirs
	}

	config, err = DeepCopier.DeepCopyConfig(config)
	if err != nil {
		return nil, errors.CouldNotDeepCopyConfig
	}

	if db == nil {
		db, err = newSQLiteDB(config)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", errors.CouldNotGetDBConnection, err)
		}
	}

	s, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("%w: %s", errors.CouldNotGetDBConnection, err)
	}
	if err = runMigrations(s, config.Workdir); err != nil {
		return nil, fmt.Errorf("%w: %s", errors.CouldNotRunMigrations, err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	return &Engine{
		config:                config,
		db:                    db,
		ctx:                   ctx,
		cancelFunc:            cancelFunc,
		processingQueue:       make(chan processingJob),
		sessionUpdatesChannel: make(chan definitions.SessionUpdate),
		writeAheadLogger:      writeAheadLogger,
		workerPool:            pond.New(config.MaxWorkers, config.MaxWorkers),
		log:                   log,
		processorFactory:      processorFactory,
	}, nil
}

func NewWithDefaults(config *config.Config, writeAheadLogger repo.WriteAheadLogger, log *logrus.Logger, db *gorm.DB, supportedProcessorsList []definitions.Processor) (*Engine, error) {
	defaultFactory := definitions.NewDefaultProcessorFactory()
	for _, processor := range supportedProcessorsList {
		defaultFactory.RegisterProcessor(processor)
	}
	return New(config, writeAheadLogger, log, defaultFactory, db)
}

func (e *Engine) Stop() {
	e.cancelFunc()
}

func (e *Engine) Submit(metadata map[string]interface{}, reader io.Reader) uuid.UUID {
	sessionID := uuid.New()
	e.workerPool.Submit(func() {
		e.processIncomingObject(&definitions.EngineIncomingObject{
			Metadata:  metadata,
			Reader:    reader,
			SessionID: sessionID,
		})
	})
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
		e.processJobs()
	}()
	return nil
}
