package engine

import (
	builtInErrors "errors"
	"fmt"
	"github.com/go-streamline/core/definitions"
	"github.com/go-streamline/core/engine/models"
	"github.com/go-streamline/core/errors"
	"github.com/go-streamline/core/repo"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func (e *Engine) executeProcessor(flow *definitions.EngineFlowObject, fileHandler definitions.EngineFileHandler, sessionID uuid.UUID, attempts int, currentNode *models.Processor) error {
	e.log.Tracef("executing processor %s for session %s", currentNode.Name, sessionID)

	processor, err := e.processorFactory.GetProcessor(currentNode.Type)
	if err != nil {
		return fmt.Errorf("failed to retrieve processor %s: %w", currentNode.Type, err)
	}

	// Log entry before execution
	logEntry := repo.LogEntry{
		SessionID:     sessionID,
		ProcessorName: currentNode.Name,
		ProcessorID:   currentNode.ID.String(),
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

	newFlow, err := processor.Execute(copiedFlow, fileHandler)
	if err != nil {
		if attempts < currentNode.MaxRetries {
			e.log.WithError(err).Warnf("retrying processor %s (%d/%d)", processor.Name(), attempts+1, currentNode.MaxRetries)
			e.scheduleNextProcessor(sessionID, fileHandler, flow, currentNode, attempts+1)
		} else {
			e.log.WithError(err).Errorf("failed to handle %s with processor %s after %d attempts", fileHandler.GetInputFile(), processor.Name(), currentNode.MaxRetries)
			e.sessionUpdatesChannel <- definitions.SessionUpdate{
				SessionID: sessionID,
				Finished:  true,
				Error:     errors.NewProcessorFailedError(currentNode.Name, err),
			}
		}
		return nil
	}

	nextProcessorNode, err := e.getNextProcessor(currentNode)
	if err != nil {
		e.log.WithError(err).Error("failed to find the next processor")
		return err
	}

	e.scheduleNextProcessor(sessionID, fileHandler, newFlow, nextProcessorNode, 0)
	return nil
}

func (e *Engine) getNextProcessor(currentProcessor *models.Processor) (*models.Processor, error) {
	var nextProcessor models.Processor
	err := e.db.Where("flow_id = ? AND flow_order > ?", currentProcessor.FlowID, currentProcessor.FlowOrder).
		Order("flow_order asc").
		First(&nextProcessor).Error
	if err != nil {
		if builtInErrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: %v", errors.FailedToGetNextProcessor, err)
	}
	return &nextProcessor, nil
}

func (e *Engine) scheduleNextProcessor(
	sessionID uuid.UUID,
	fileHandler definitions.EngineFileHandler,
	flow *definitions.EngineFlowObject,
	currentNode *models.Processor,
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
