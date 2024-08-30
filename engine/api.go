package engine

import (
	"context"
	"fmt"
	"github.com/alitto/pond"
	"github.com/go-streamline/core/flow/persist"
	"github.com/go-streamline/core/processors"
	"github.com/go-streamline/engine/config"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/go-streamline/interfaces/utils"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"io"
)

var ErrCouldNotCreateFlowManager = fmt.Errorf("could not create flow manager")
var ErrCouldNotCreateDirs = fmt.Errorf("failed to create work directories")
var ErrRecoveryFailed = fmt.Errorf("failed to recover, if you don't want to recover, please delete the WAL file or set IgnoreRecoveryErrors to true")
var ErrCouldNotDeepCopyConfig = fmt.Errorf("could not deep copy config")

type Engine struct {
	config                *config.Config
	ctx                   context.Context
	cancelFunc            context.CancelFunc
	processingQueue       chan processingJob
	sessionUpdatesChannel chan definitions.SessionUpdate
	writeAheadLogger      definitions.WriteAheadLogger
	workerPool            *pond.WorkerPool
	log                   *logrus.Logger
	processorFactory      definitions.ProcessorFactory
	flowManager           definitions.FlowManager
}

type processingJob struct {
	sessionID   uuid.UUID
	attempts    int
	flow        *definitions.EngineFlowObject
	fileHandler definitions.EngineFileHandler
	currentNode *definitions.SimpleProcessor
}

func New(config *config.Config, writeAheadLogger definitions.WriteAheadLogger, log *logrus.Logger, processorFactory definitions.ProcessorFactory, flowManager definitions.FlowManager) (*Engine, error) {
	err := utils.CreateDirsIfNotExist(config.Workdir)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCouldNotCreateDirs, err)
	}

	config, err = DeepCopier.DeepCopyConfig(config)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCouldNotDeepCopyConfig, err)
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

func NewWithDefaults(config *config.Config, writeAheadLogger definitions.WriteAheadLogger, log *logrus.Logger, db *gorm.DB, supportedProcessorsList []definitions.Processor) (*Engine, error) {
	defaultFactory := processors.NewDefaultProcessorFactory()
	for _, processor := range supportedProcessorsList {
		defaultFactory.RegisterProcessor(processor)
	}
	flowManager := persist.NewDefaultFlowManager(db)
	return New(config, writeAheadLogger, log, defaultFactory, flowManager)
}

func (e *Engine) Stop() {
	e.cancelFunc()
}

func (e *Engine) Submit(flowID uuid.UUID, metadata map[string]interface{}, reader io.Reader) uuid.UUID {
	sessionID := uuid.New()
	e.workerPool.Submit(func() {
		e.processIncomingObject(flowID, &definitions.EngineIncomingObject{
			FlowID:    flowID,
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
	if err != nil {
		return ErrRecoveryFailed
	}
	go func() {
		e.processJobs()
	}()
	return nil
}
