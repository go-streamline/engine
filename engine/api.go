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

func New(config *config.Config, writeAheadLogger repo.WriteAheadLogger, log *logrus.Logger) (*Engine, error) {
	err := utils.CreateDirsIfNotExist(config.Workdir)
	if err != nil {
		return nil, errors.CouldNotCreateDirs
	}

	config, err = DeepCopier.DeepCopyConfig(config)
	if err != nil {
		return nil, errors.CouldNotDeepCopyConfig
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	var head *processorNode
	var tail *processorNode
	for _, procConfig := range config.Processors {
		node := &processorNode{ProcessorConfig: procConfig}
		if head == nil {
			head = node
			tail = node
		} else {
			tail.Next = node
			tail = node
		}
	}

	return &Engine{
		config:                config,
		ProcessorListHead:     head,
		ctx:                   ctx,
		cancelFunc:            cancelFunc,
		processingQueue:       make(chan processingJob),
		sessionUpdatesChannel: make(chan definitions.SessionUpdate),
		writeAheadLogger:      writeAheadLogger,
		workerPool:            pond.New(config.MaxWorkers, config.MaxWorkers),
		log:                   log,
	}, nil
}

func NewWithDefaults(writeAheadLogger repo.WriteAheadLogger, log *logrus.Logger, processors []config.ProcessorConfig) (*Engine, error) {
	return New(
		&config.Config{
			MaxWorkers: 10,
			Workdir:    "/tmp/go-streamline",
			Processors: processors,
		}, writeAheadLogger, log)
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
