package engine

import (
	"context"
	"github.com/alitto/pond"
	"github.com/go-streamline/core/config"
	"github.com/go-streamline/core/definitions"
	"github.com/go-streamline/core/errors"
	"github.com/go-streamline/core/filehandler"
	"github.com/go-streamline/core/repo"
	"github.com/go-streamline/core/utils"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"path"
	"time"
)

type Engine struct {
	Handlers              []config.HandlerConfig
	ctx                   context.Context
	incomingQueue         chan definitions.EngineIncomingObject
	sessionUpdatesChannel chan definitions.SessionUpdate
	contentsDir           string
	writeAheadLogger      repo.WriteAheadLogger
	IgnoreRecoveryErrors  bool
	workerPool            *pond.WorkerPool
	log                   *logrus.Logger
	retryQueue            chan retryTask
}

type retryTask struct {
	flow        *definitions.EngineFlowObject
	fileHandler definitions.EngineFileHandler
	handlerID   string
	sessionID   uuid.UUID
	attempts    int
}

func New(ctx context.Context, config config.Config, writeAheadLogger repo.WriteAheadLogger, log *logrus.Logger) (*Engine, error) {
	err := utils.CreateDirsIfNotExist(config.Workdir)
	if err != nil {
		return nil, errors.CouldNotCreateDirs
	}

	return &Engine{
		Handlers:              config.Handlers,
		ctx:                   ctx,
		incomingQueue:         make(chan definitions.EngineIncomingObject),
		sessionUpdatesChannel: make(chan definitions.SessionUpdate),
		contentsDir:           path.Join(config.Workdir, "contents"),
		writeAheadLogger:      writeAheadLogger,
		IgnoreRecoveryErrors:  config.IgnoreRecoveryErrors,
		workerPool:            pond.New(config.MaxWorkers, config.MaxWorkers),
		log:                   log,
		retryQueue:            make(chan retryTask, config.MaxWorkers),
	}, nil
}

func (e *Engine) Submit(i definitions.EngineIncomingObject) {
	e.incomingQueue <- i
}

func (e *Engine) SessionUpdates() <-chan definitions.SessionUpdate {
	return e.sessionUpdatesChannel
}

func (e *Engine) handleFiles() {
	for {
		select {
		case <-e.ctx.Done():
			e.log.Infof("stopping worker")
			return
		case i := <-e.incomingQueue:
			e.workerPool.Submit(func() {
				e.handleFile(i)
			})
		case task := <-e.retryQueue:
			e.workerPool.Submit(func() {
				e.retryTask(task)
			})
		}
	}
}

func (e *Engine) Run() error {
	err := e.Recover()
	if err != nil && !e.IgnoreRecoveryErrors {
		return errors.RecoveryError
	}
	go func() {
		e.handleFiles()
	}()
	return nil
}

func transformIncomingObjectToFlowObject(i definitions.EngineIncomingObject) definitions.EngineFlowObject {
	return definitions.EngineFlowObject{
		Metadata: i.Metadata,
	}
}

func (e *Engine) handleFile(i definitions.EngineIncomingObject) {
	var err error
	sessionID := uuid.New()
	e.log.Debugf("handling file %s with sessionID %s", i.Filepath, sessionID)

	flow := transformIncomingObjectToFlowObject(i)
	input := path.Join(e.contentsDir, uuid.NewString())

	walEntry := repo.LogEntry{
		SessionID:   sessionID,
		HandlerName: "__init__",
		HandlerID:   "__init__",
		InputFile:   i.Filepath,
		OutputFile:  input,
		FlowObject:  flow,
	}
	e.writeAheadLogger.WriteEntry(walEntry)

	err = utils.CopyFile(i.Filepath, input)
	if err != nil {
		e.log.WithError(err).Errorf("failed to copy file %s to contents folder", i.Filepath)
		e.scheduleInitRetry(i, sessionID)
		return
	}

	fileHandler := filehandler.NewEngineFileHandler(input)

	err = e.processHandlers(&flow, fileHandler, "", sessionID)
	if err != nil {
		e.log.WithError(err).Error("failed to process handlers")
		walEntry.HandlerName = "__end__"
		walEntry.HandlerID = "__end__"
		e.writeAheadLogger.WriteEntry(walEntry)
		e.sessionUpdatesChannel <- definitions.SessionUpdate{
			SessionID: sessionID,
			Finished:  true,
			Error:     err,
		}
		return
	}

	e.sessionUpdatesChannel <- definitions.SessionUpdate{
		SessionID: sessionID,
		Finished:  true,
		Error:     nil,
	}
}

func (e *Engine) scheduleInitRetry(i definitions.EngineIncomingObject, sessionID uuid.UUID) {
	// Log the retry attempt in the WAL
	e.log.Infof("scheduling retry for init of session %s", sessionID)
	walEntry := repo.LogEntry{
		SessionID:   sessionID,
		HandlerName: "__init__",
		HandlerID:   "__init__",
		InputFile:   i.Filepath,
		OutputFile:  "",
		FlowObject:  transformIncomingObjectToFlowObject(i),
		RetryCount:  0, // No retry limit for init
	}
	e.writeAheadLogger.WriteEntry(walEntry)

	time.AfterFunc(time.Second*5, func() {
		e.retryQueue <- retryTask{
			flow:        &walEntry.FlowObject,
			fileHandler: filehandler.NewEngineFileHandler(i.Filepath), // Retry from the original file path
			handlerID:   "__init__",
			sessionID:   sessionID,
			attempts:    0, // No limit on attempts for init
		}
	})
}
