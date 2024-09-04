package engine

import (
	"context"
	"errors"
	"fmt"
	"github.com/alitto/pond"
	"github.com/go-streamline/core/flow/persist"
	"github.com/go-streamline/engine/config"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/go-streamline/interfaces/utils"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
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
	activeFlows           map[uuid.UUID]*definitions.Flow
	triggerProcessors     map[uuid.UUID]definitions.TriggerProcessor
	scheduler             *cron.Cron
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

	return &Engine{
		config:                config,
		processingQueue:       make(chan processingJob),
		sessionUpdatesChannel: make(chan definitions.SessionUpdate),
		writeAheadLogger:      writeAheadLogger,
		workerPool:            pond.New(config.MaxWorkers, config.MaxWorkers),
		log:                   log,
		processorFactory:      processorFactory,
		flowManager:           flowManager,
		activeFlows:           make(map[uuid.UUID]*definitions.Flow),
		triggerProcessors:     make(map[uuid.UUID]definitions.TriggerProcessor),
		scheduler:             cron.New(cron.WithSeconds()),
	}, nil
}

func NewWithDefaults(config *config.Config, writeAheadLogger definitions.WriteAheadLogger, log *logrus.Logger, db *gorm.DB, processorFactory definitions.ProcessorFactory) (*Engine, error) {
	flowManager, err := persist.NewDBFlowManager(db)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCouldNotCreateFlowManager, err)
	}
	return New(config, writeAheadLogger, log, processorFactory, flowManager)
}

func (e *Engine) Close() error {
	e.cancelFunc()
	e.workerPool.StopAndWait()
	var errs []error
	for _, triggerProcessor := range e.triggerProcessors {
		err := triggerProcessor.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (e *Engine) SessionUpdates() <-chan definitions.SessionUpdate {
	return e.sessionUpdatesChannel
}

func (e *Engine) Run() error {
	e.ctx, e.cancelFunc = context.WithCancel(context.Background())
	err := e.recover()
	if err != nil {
		return ErrRecoveryFailed
	}
	go e.monitorFlows()
	go func() {
		e.processJobs()
	}()
	return nil
}
