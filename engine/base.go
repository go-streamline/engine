package engine

import (
	"fmt"
	"github.com/go-streamline/core/filehandler"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/google/uuid"
	"path"
)

var ErrFailedToExecuteProcessors = fmt.Errorf("failed to execute processors")

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

func (e *Engine) runTriggerProcessor(tp definitions.TriggerProcessor, triggerProcessorDef *definitions.SimpleTriggerProcessor, flow *definitions.Flow) {
	for {
		select {
		case <-e.ctx.Done():
			return
		default:
			outputFile := path.Join(e.config.Workdir, "contents", uuid.New().String())
			fileHandler := filehandler.NewWriteOnlyEngineFileHandler(outputFile)
			flowObject, err := tp.Execute(&definitions.EngineFlowObject{}, fileHandler, e.log)
			if err != nil {
				e.log.WithError(err).Errorf("failed to execute trigger processor %s in flow %s", triggerProcessorDef.Name, flow.ID)
				continue
			}
			e.scheduleNextProcessor(uuid.New(), fileHandler, flowObject, nil, 0)
		}
	}
}
