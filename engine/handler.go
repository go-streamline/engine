package engine

import (
	"fmt"
	"github.com/go-streamline/core/definitions"
	"github.com/go-streamline/core/errors"
	"github.com/go-streamline/core/repo"
	"github.com/google/uuid"
)

func (e *Engine) executeProcessor(flow *definitions.EngineFlowObject, fileHandler definitions.EngineFileHandler, sessionID uuid.UUID, attempts int, currentNode *processorNode) error {
	e.log.Tracef("executing processor %s for session %s", currentNode.ProcessorConfig.Processor.GetID(), sessionID)

	pCtx := currentNode.ProcessorConfig

	// Log entry before execution
	logEntry := repo.LogEntry{
		SessionID:     sessionID,
		ProcessorName: pCtx.Processor.Name(),
		ProcessorID:   pCtx.Processor.GetID(),
		InputFile:     fileHandler.GetInputFile(),
		OutputFile:    fileHandler.GetOutputFile(),
		FlowObject:    *flow,
	}
	e.writeAheadLogger.WriteEntry(logEntry)

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

	newFlow, err := pCtx.Processor.Execute(copiedFlow, fileHandler)
	if err != nil {
		if attempts < pCtx.Retry.MaxRetries {
			e.log.WithError(err).Warnf("retrying processor %s (%d/%d)", pCtx.Processor.Name(), attempts+1, pCtx.Retry.MaxRetries)
			e.scheduleNextProcessor(sessionID, fileHandler, flow, currentNode, attempts+1)
		} else {
			e.log.WithError(err).Errorf("failed to handle %s with processor %s after %d attempts", fileHandler.GetInputFile(), pCtx.Processor.Name(), pCtx.Retry.MaxRetries)
			e.sessionUpdatesChannel <- definitions.SessionUpdate{
				SessionID: sessionID,
				Finished:  true,
				Error:     fmt.Errorf("%w: processor %s failed: %v", errors.ProcessorFailed, pCtx.Processor.Name(), err),
			}
		}
		return nil
	}

	e.scheduleNextProcessor(sessionID, fileHandler, newFlow, currentNode.Next, 0)
	return nil
}

func (e *Engine) scheduleNextProcessor(
	sessionID uuid.UUID,
	fileHandler definitions.EngineFileHandler,
	flow *definitions.EngineFlowObject,
	currentNode *processorNode,
	attempts int,
) {
	if currentNode == nil {
		e.sessionUpdatesChannel <- definitions.SessionUpdate{
			SessionID: sessionID,
			Finished:  true,
			Error:     nil,
		}
		return
	}

	e.processingQueue <- processingJob{
		sessionID:   sessionID,
		attempts:    attempts,
		flow:        flow,
		fileHandler: fileHandler,
		currentNode: currentNode,
	}
}
