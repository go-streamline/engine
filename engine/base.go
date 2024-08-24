package engine

import (
	"context"
	"fmt"
	"github.com/alitto/pond"
	"github.com/go-streamline/core/config"
	"github.com/go-streamline/core/definitions"
	"github.com/go-streamline/core/errors"
	"github.com/go-streamline/core/filehandler"
	"github.com/go-streamline/core/repo"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"path"
)

type Engine struct {
	config                *config.Config
	Processors            []config.ProcessorConfig
	ctx                   context.Context
	incomingQueue         chan definitions.EngineIncomingObject
	sessionUpdatesChannel chan definitions.SessionUpdate
	contentsDir           string
	writeAheadLogger      repo.WriteAheadLogger
	ignoreRecoveryErrors  bool
	workerPool            *pond.WorkerPool
	log                   *logrus.Logger
	retryQueue            chan retryTask
}

type retryTask struct {
	flow        *definitions.EngineFlowObject
	fileHandler definitions.EngineFileHandler
	processorID string
	sessionID   uuid.UUID
	attempts    int
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

func transformIncomingObjectToFlowObject(i definitions.EngineIncomingObject) definitions.EngineFlowObject {
	return definitions.EngineFlowObject{
		Metadata: i.Metadata,
	}
}

func (e *Engine) handleFile(i definitions.EngineIncomingObject) {
	sessionID := i.SessionID
	e.log.Debugf("handling sessionID %s", sessionID)

	flow := transformIncomingObjectToFlowObject(i)
	input := path.Join(e.contentsDir, uuid.NewString())

	file, err := os.Create(input)
	if err != nil {
		e.log.WithError(err).Errorf("failed to create file %s", input)
		e.sessionUpdatesChannel <- definitions.SessionUpdate{
			SessionID: sessionID,
			Finished:  true,
			Error:     fmt.Errorf("%w: %v", errors.FailedToCreateFile, err),
		}
		return
	}
	defer file.Close()

	_, err = io.Copy(file, i.Reader)
	if err != nil {
		e.log.WithError(err).Error("failed to copy data to file")
		e.sessionUpdatesChannel <- definitions.SessionUpdate{
			SessionID: sessionID,
			Finished:  true,
			Error:     fmt.Errorf("%w: %v", errors.FailedToCopyJobToContentsFolder, err),
		}
		return
	}

	fileHandler := filehandler.NewEngineFileHandler(input)

	err = e.executeProcessors(&flow, fileHandler, "", sessionID)
	if err != nil {
		e.log.WithError(err).Error("failed to execute processors")
		e.sessionUpdatesChannel <- definitions.SessionUpdate{
			SessionID: sessionID,
			Finished:  true,
			Error:     fmt.Errorf("%w: %v", errors.FailedToExecuteProcessors, err),
		}
		return
	}

	e.sessionUpdatesChannel <- definitions.SessionUpdate{
		SessionID: sessionID,
		Finished:  true,
		Error:     nil,
	}
}
