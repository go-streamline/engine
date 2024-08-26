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

// New creates a new instance of Engine, may return the following errors: CouldNotCreateDirs, CouldNotDeepCopyConfig /**
func New(config *config.Config, writeAheadLogger repo.WriteAheadLogger, log *logrus.Logger, processorFactory definitions.ProcessorFactory, flowManager definitions.FlowManager) (*Engine, error) {
	err := utils.CreateDirsIfNotExist(config.Workdir)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errors.CouldNotCreateDirs, err)
	}

	config, err = DeepCopier.DeepCopyConfig(config)
	if err != nil {
		return nil, errors.CouldNotDeepCopyConfig
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	return &Engine{
		config:                config,
		ctx:                   ctx,
		cancelFunc:            cancelFunc,
		processingQueue:       make(chan processingJob),
		sessionUpdatesChannel: make(chan definitions.SessionUpdate),
		writeAheadLogger:      writeAheadLogger,
		workerPool:            pond.New(config.MaxWorkers, config.MaxWorkers),
		log:                   log,
		processorFactory:      processorFactory,
		flowManager:           flowManager,
	}, nil
}

// NewWithDefaults creates Engine with as least effort as possible. Will create a default flow manager using db and return any error it my return wrapped in CouldNotCreateFlowManager.  /**
func NewWithDefaults(config *config.Config, writeAheadLogger repo.WriteAheadLogger, log *logrus.Logger, db *gorm.DB, supportedProcessorsList []definitions.Processor) (*Engine, error) {
	defaultFactory := definitions.NewDefaultProcessorFactory()
	for _, processor := range supportedProcessorsList {
		defaultFactory.RegisterProcessor(processor)
	}
	flowManager, err := repo.NewDefaultFlowManager(db, config.Workdir)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errors.CouldNotCreateFlowManager, err)
	}
	return New(config, writeAheadLogger, log, defaultFactory, flowManager)
}

func (e *Engine) Stop() {
	e.cancelFunc()
}

func (e *Engine) Submit(flowID uuid.UUID, metadata map[string]interface{}, reader io.Reader) uuid.UUID {
	sessionID := uuid.New()
	e.workerPool.Submit(func() {
		e.processIncomingObject(flowID, &definitions.EngineIncomingObject{
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
