package engine

import (
	"context"
	"fmt"
	"github.com/alitto/pond"
	"github.com/go-streamline/core/config"
	"github.com/go-streamline/core/definitions"
	"github.com/go-streamline/core/filehandler"
	"github.com/go-streamline/core/models"
	"github.com/go-streamline/core/repo"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"path"
)

var ErrFailedToCreateFile = fmt.Errorf("failed to create initial job file")
var ErrFailedToCopyJobToContentsFolder = fmt.Errorf("failed to copy job to contents folder")
var ErrFailedToExecuteProcessors = fmt.Errorf("failed to execute processors")
var ErrNoProcessorsAvailable = fmt.Errorf("no processors available")

type Engine struct {
	config                *config.Config
	ctx                   context.Context
	cancelFunc            context.CancelFunc
	processingQueue       chan processingJob
	sessionUpdatesChannel chan definitions.SessionUpdate
	contentsDir           string
	writeAheadLogger      repo.WriteAheadLogger
	ignoreRecoveryErrors  bool
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
	currentNode *models.Processor
}

func (e *Engine) processJobs() {
	for {
		select {
		case <-e.ctx.Done():
			e.log.Infof("stopping processor")
			e.workerPool.Stop()
			return
		case job := <-e.processingQueue:
			e.workerPool.Submit(func() {
				e.processJob(job)
			})
		}
	}
}

func transformIncomingObjectToFlowObject(i *definitions.EngineIncomingObject) *definitions.EngineFlowObject {
	return &definitions.EngineFlowObject{
		Metadata: i.Metadata,
	}
}

func (e *Engine) processIncomingObject(flowID uuid.UUID, i *definitions.EngineIncomingObject) {
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
			Error:     fmt.Errorf("%w: %v", ErrFailedToCreateFile, err),
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
			Error:     fmt.Errorf("%w: %v", ErrFailedToCopyJobToContentsFolder, err),
		}
		return
	}

	fileHandler := filehandler.NewEngineFileHandler(input)
	firstProcessor, err := e.flowManager.GetFirstProcessorForFlow(flowID)
	if err != nil {
		e.log.WithError(err).Error("failed to get first processor for flow")
		e.sessionUpdatesChannel <- definitions.SessionUpdate{
			SessionID: sessionID,
			Finished:  true,
			Error:     err,
		}
		return
	}

	if firstProcessor != nil {
		e.scheduleNextProcessor(sessionID, fileHandler, flow, firstProcessor, 0)
	} else {
		e.log.Warn("No processors available to handle the job")
		e.sessionUpdatesChannel <- definitions.SessionUpdate{
			SessionID: sessionID,
			Finished:  true,
			Error:     ErrNoProcessorsAvailable,
		}
	}
}

func (e *Engine) processJob(job processingJob) {
	err := e.executeProcessor(job.flow, job.fileHandler, job.sessionID, job.attempts, job.currentNode)
	if err != nil {
		e.log.WithError(err).Errorf("failed to execute processor for session %s", job.sessionID)
		e.sessionUpdatesChannel <- definitions.SessionUpdate{
			SessionID: job.sessionID,
			Finished:  true,
			Error:     fmt.Errorf("%w: %v", ErrFailedToExecuteProcessors, err),
		}
	}
}
