package engine

import (
	"fmt"
	"github.com/go-streamline/core/config"
	"github.com/go-streamline/core/definitions"
	"github.com/go-streamline/core/repo"
	"github.com/go-streamline/core/utils"
	"github.com/google/uuid"
	"os"
	"time"
)

func (e *Engine) processHandlers(flow *definitions.EngineFlowObject, fileHandler definitions.EngineFileHandler, startHandlerID string, sessionID uuid.UUID) error {
	e.log.Tracef("processing handlers")
	resume := startHandlerID == ""
	e.log.Debugf("resuming from handler %s", startHandlerID)

	for _, hCtx := range e.Handlers {
		h := hCtx.Handler
		handlerID := h.GetID()
		if handlerID == startHandlerID && !resume {
			resume = true
		}
		if resume {
			e.log.Debugf("handling %s with handler %s", fileHandler.GetInputFile(), h.Name())
			logEntry := repo.LogEntry{
				SessionID:   sessionID,
				HandlerName: h.Name(),
				HandlerID:   handlerID,
				InputFile:   fileHandler.GetInputFile(),
				OutputFile:  fileHandler.GetOutputFile(),
				FlowObject:  *flow,
			}
			e.log.Debugf("writing WAL entry for handler %s (%s)", h.Name(), handlerID)
			e.writeAheadLogger.WriteEntry(logEntry)
			e.log.Debugf("deep copying flow object for handler %s (%s)", h.Name(), handlerID)

			copiedFlow, err := utils.DeepCopy(flow)
			if err != nil {
				e.log.WithError(err).Error("failed to copy flow object")
				e.sessionUpdatesChannel <- definitions.SessionUpdate{
					SessionID: sessionID,
					Finished:  false,
					Error:     err,
				}
				return err
			}

			e.log.Debugf("handling %s with handler %s", fileHandler.GetInputFile(), h.Name())

			// Handle without retry loop
			newFlow, err := h.Handle(copiedFlow, fileHandler)
			if err != nil {
				e.log.WithError(err).Warnf("handler %s failed, scheduling retry", h.Name())
				e.scheduleRetry(copiedFlow, fileHandler, handlerID, sessionID, hCtx.Retry.BackOffInterval, 1)
				return nil
			}

			flow = newFlow
			e.log.Debugf("handled %s with handler %s", fileHandler.GetInputFile(), h.Name())

			fileHandler, err = fileHandler.GenerateNewFileHandler()
			if err != nil {
				e.log.WithError(err).Errorf("failed to generate new file handler for handler %s", h.Name())
				e.sessionUpdatesChannel <- definitions.SessionUpdate{
					SessionID: sessionID,
					Finished:  false,
					Error:     err,
				}
				return err
			}
		}
	}

	if !resume {
		e.log.Warnf("no handlers were processed, go-streamline will not write the output file")
	}

	inputFile := fileHandler.GetInputFile()
	logEntry := repo.LogEntry{
		SessionID:   sessionID,
		HandlerName: "__end__",
		HandlerID:   "__end__",
		InputFile:   inputFile,
		OutputFile:  fileHandler.GetOutputFile(),
		FlowObject:  *flow,
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
	handlerID string,
	sessionID uuid.UUID,
	backOffInterval time.Duration,
	attempts int,
) {
	// Log the retry attempt in the WAL
	logEntry := repo.LogEntry{
		SessionID:   sessionID,
		HandlerName: "__retry__",
		HandlerID:   handlerID,
		InputFile:   fileHandler.GetInputFile(),
		OutputFile:  fileHandler.GetOutputFile(),
		FlowObject:  *flow,
		RetryCount:  attempts,
	}
	e.writeAheadLogger.WriteEntry(logEntry)

	// Schedule the retry
	time.AfterFunc(backOffInterval, func() {
		e.retryQueue <- retryTask{
			flow:        flow,
			fileHandler: fileHandler,
			handlerID:   handlerID,
			sessionID:   sessionID,
			attempts:    attempts,
		}
	})
}

func (e *Engine) retryTask(task retryTask) {
	if task.handlerID == "__init__" {
		// Handle the init retry
		e.log.Debugf("retrying init for session %s", task.sessionID)

		// Attempt the init process again
		i := definitions.EngineIncomingObject{
			Filepath: task.fileHandler.GetInputFile(),
			Metadata: task.flow.Metadata,
		}
		e.handleFile(i)
	} else {
		// Existing logic for other handlers
		hCtx := e.findHandlerContext(task.handlerID)
		if hCtx == nil {
			e.log.Errorf("handler with ID %s not found during retry", task.handlerID)
			e.sessionUpdatesChannel <- definitions.SessionUpdate{
				SessionID: task.sessionID,
				Finished:  true,
				Error:     fmt.Errorf("handler with ID %s not found", task.handlerID),
			}
			return
		}

		e.log.Debugf("retrying handler %s for session %s, attempt %d", task.handlerID, task.sessionID, task.attempts)

		newFlow, err := hCtx.Handler.Handle(task.flow, task.fileHandler)
		if err != nil {
			if task.attempts < hCtx.Retry.MaxRetries {
				e.log.WithError(err).Warnf("retrying handler %s (%d/%d)", hCtx.Handler.Name(), task.attempts+1, hCtx.Retry.MaxRetries)
				e.scheduleRetry(task.flow, task.fileHandler, task.handlerID, task.sessionID, hCtx.Retry.BackOffInterval, task.attempts+1)
				e.sessionUpdatesChannel <- definitions.SessionUpdate{
					SessionID: task.sessionID,
					Finished:  false,
					Error:     err,
				}
			} else {
				e.log.WithError(err).Errorf("failed to handle %s with handler %s after %d attempts", task.fileHandler.GetInputFile(), hCtx.Handler.Name(), hCtx.Retry.MaxRetries)
				e.sessionUpdatesChannel <- definitions.SessionUpdate{
					SessionID: task.sessionID,
					Finished:  true,
					Error:     err,
				}
			}
		} else {
			err = e.processHandlers(newFlow, task.fileHandler, task.handlerID, task.sessionID)
			if err != nil {
				e.log.WithError(err).Errorf("failed to process handlers for session %s", task.sessionID)
				e.sessionUpdatesChannel <- definitions.SessionUpdate{
					SessionID: task.sessionID,
					Finished:  true,
					Error:     err,
				}
			}
		}
	}
}

func (e *Engine) findHandlerContext(handlerID string) *config.HandlerConfig {
	for _, hCtx := range e.Handlers {
		if hCtx.Handler.GetID() == handlerID {
			return &hCtx
		}
	}
	return nil
}
