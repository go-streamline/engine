package engine

import (
	"fmt"
	"github.com/go-streamline/core/config"
	"github.com/go-streamline/core/definitions"
	"github.com/go-streamline/core/errors"
	"github.com/go-streamline/core/repo"
	"github.com/google/uuid"
	"os"
	"time"
)

func (e *Engine) executeProcessors(flow *definitions.EngineFlowObject, fileHandler definitions.EngineFileHandler, startProcesosrID string, sessionID uuid.UUID) error {
	e.log.Tracef("executing processors for session %s", sessionID)
	resume := startProcesosrID == ""
	e.log.Debugf("resuming from processor %s", startProcesosrID)

	for _, hCtx := range e.Processors {
		h := hCtx.Processor
		processorID := h.GetID()
		if processorID == startProcesosrID && !resume {
			resume = true
		}
		if resume {
			e.log.Debugf("handling %s with processor %s", fileHandler.GetInputFile(), h.Name())
			logEntry := repo.LogEntry{
				SessionID:     sessionID,
				ProcessorName: h.Name(),
				ProcessorID:   processorID,
				InputFile:     fileHandler.GetInputFile(),
				OutputFile:    fileHandler.GetOutputFile(),
				FlowObject:    *flow,
			}
			e.log.Debugf("writing WAL entry for processor %s (%s)", h.Name(), processorID)
			e.writeAheadLogger.WriteEntry(logEntry)
			e.log.Debugf("deep copying flow object for processor %s (%s)", h.Name(), processorID)

			copiedFlow, err := DeepCopier.DeepCopyFlowObject(flow)
			if err != nil {
				e.log.WithError(err).Error("failed to copy flow object")
				e.sessionUpdatesChannel <- definitions.SessionUpdate{
					SessionID: sessionID,
					Finished:  false,
					Error:     fmt.Errorf("%w: %v", errors.CouldNotDeepCopyFlowObject, err),
				}
				return err
			}

			e.log.Debugf("handling %s with processor %s", fileHandler.GetInputFile(), h.Name())

			newFlow, err := h.Execute(copiedFlow, fileHandler)
			if err != nil {
				e.log.WithError(err).Warnf("processor %s failed, scheduling retry", h.Name())
				e.scheduleRetry(copiedFlow, fileHandler, processorID, sessionID, hCtx.Retry.BackOffInterval, 1)
				return nil
			}

			flow = newFlow
			e.log.Debugf("handled %s with processor %s", fileHandler.GetInputFile(), h.Name())

			fileHandler, err = fileHandler.GenerateNewFileHandler()
			if err != nil {
				e.log.WithError(err).Errorf("failed to generate new file handler for processor %s", h.Name())
				e.sessionUpdatesChannel <- definitions.SessionUpdate{
					SessionID: sessionID,
					Finished:  false,
					Error:     fmt.Errorf("%w: %v", errors.FailedToGenerateNewFileHandler, err),
				}
				return err
			}
		}
	}

	if !resume {
		e.log.Warnf("no processor was executed, go-streamline will not write the output file")
	}

	inputFile := fileHandler.GetInputFile()
	logEntry := repo.LogEntry{
		SessionID:     sessionID,
		ProcessorName: "__end__",
		ProcessorID:   "__end__",
		InputFile:     inputFile,
		OutputFile:    fileHandler.GetOutputFile(),
		FlowObject:    *flow,
	}
	e.writeAheadLogger.WriteEntry(logEntry)
	err := os.Remove(inputFile)
	if err != nil {
		e.log.WithError(err).Warnf("failed to remove final input file %s", inputFile)
	}

	e.sessionUpdatesChannel <- definitions.SessionUpdate{
		SessionID: sessionID,
		Finished:  true,
		Error:     nil,
	}
	e.log.Infof("go-streamline finished processing handlers for file %s", inputFile)

	return nil
}

func (e *Engine) scheduleRetry(
	flow *definitions.EngineFlowObject,
	fileHandler definitions.EngineFileHandler,
	processorID string,
	sessionID uuid.UUID,
	backOffInterval time.Duration,
	attempts int,
) {
	logEntry := repo.LogEntry{
		SessionID:     sessionID,
		ProcessorName: "__retry__",
		ProcessorID:   processorID,
		InputFile:     fileHandler.GetInputFile(),
		OutputFile:    fileHandler.GetOutputFile(),
		FlowObject:    *flow,
		RetryCount:    attempts,
	}
	e.writeAheadLogger.WriteEntry(logEntry)

	time.AfterFunc(backOffInterval, func() {
		e.retryQueue <- retryTask{
			flow:        flow,
			fileHandler: fileHandler,
			processorID: processorID,
			sessionID:   sessionID,
			attempts:    attempts,
		}
	})
}

func (e *Engine) retryTask(task retryTask) {
	pCtx := e.findHandlerContext(task.processorID)
	if pCtx == nil {
		e.log.Errorf("processor with ID %s not found during retry", task.processorID)
		e.sessionUpdatesChannel <- definitions.SessionUpdate{
			SessionID: task.sessionID,
			Finished:  true,
			Error:     fmt.Errorf("%w: %s", errors.ProcessorNotFound, task.processorID),
		}
		return
	}

	e.log.Debugf("retrying processor %s for session %s, attempt %d", task.processorID, task.sessionID, task.attempts)

	newFlow, err := pCtx.Processor.Execute(task.flow, task.fileHandler)
	if err != nil {
		finalErr := fmt.Errorf("%w: procssor %s failed: %v", errors.ProcessorFailed, pCtx.Processor, err)
		if task.attempts < pCtx.Retry.MaxRetries {
			e.log.WithError(err).Warnf("retrying processor %s (%d/%d)", pCtx.Processor.Name(), task.attempts+1, pCtx.Retry.MaxRetries)
			e.scheduleRetry(task.flow, task.fileHandler, task.processorID, task.sessionID, pCtx.Retry.BackOffInterval, task.attempts+1)
			e.sessionUpdatesChannel <- definitions.SessionUpdate{
				SessionID: task.sessionID,
				Finished:  false,
				Error:     finalErr,
			}
		} else {
			e.log.WithError(err).Errorf("failed to handle %s with processor %s after %d attempts", task.fileHandler.GetInputFile(), pCtx.Processor.Name(), pCtx.Retry.MaxRetries)
			e.sessionUpdatesChannel <- definitions.SessionUpdate{
				SessionID: task.sessionID,
				Finished:  true,
				Error:     finalErr,
			}
		}
	} else {
		err = e.executeProcessors(newFlow, task.fileHandler, task.processorID, task.sessionID)
		if err != nil {
			e.log.WithError(err).Errorf("failed to execute processors for session %s", task.sessionID)
			e.sessionUpdatesChannel <- definitions.SessionUpdate{
				SessionID: task.sessionID,
				Finished:  true,
				Error:     fmt.Errorf("%w: %v", errors.FailedToExecuteProcessors, err),
			}
		}
	}
}

func (e *Engine) findHandlerContext(handlerID string) *config.ProcessorConfig {
	for _, hCtx := range e.Processors {
		if hCtx.Processor.GetID() == handlerID {
			return &hCtx
		}
	}
	return nil
}
