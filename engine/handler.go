package engine

import (
	"fmt"
	"github.com/go-streamline/core/definitions"
	"github.com/go-streamline/core/models"
	"github.com/go-streamline/core/repo"
	"github.com/go-streamline/core/utils"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"path"
)

var ErrCouldNotDeepCopyFlowObject = fmt.Errorf("could not deep copy flow object")
var ErrFailedToGetNextProcessor = fmt.Errorf("failed to get next processor")
var ErrFailedToSetProcessorConfig = fmt.Errorf("failed to set processor configuration")

func (e *Engine) executeProcessor(flow *definitions.EngineFlowObject, fileHandler definitions.EngineFileHandler, sessionID uuid.UUID, attempts int, currentNode *models.Processor) error {

	processor, err := e.processorFactory.GetProcessor(currentNode.Type)
	if err != nil {
		return fmt.Errorf("failed to retrieve processor %s: %w", currentNode.Type, err)
	}

	err = processor.SetConfig(currentNode.Configuration)
	if err != nil {
		e.log.WithError(err).Error("failed to set processor configuration")
		return fmt.Errorf("%w: %v", ErrFailedToSetProcessorConfig, err)
	}

	logEntry := repo.LogEntry{
		// log entry before execution
		// create the logger for this processor
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
		return fmt.Errorf("%w: %v", ErrCouldNotDeepCopyFlowObject, err)
	}

	logger, err := e.createProcessorLogger(sessionID, currentNode)
	if err != nil {
		e.log.WithError(err).Errorf("failed to create logger for processor %s", currentNode.Name)
		return newFailedToCreateLoggerError(currentNode.Name, err)
	}
	newFlow, err := processor.Execute(copiedFlow, fileHandler, logger)
	if err != nil {
		if attempts < currentNode.MaxRetries {
			logger.WithError(err).Warnf("retrying processor %s (%d/%d)", processor.Name(), attempts+1, currentNode.MaxRetries)
			e.scheduleNextProcessor(sessionID, fileHandler, flow, currentNode, attempts+1)
		} else {
			logger.WithError(err).Errorf("failed to handle %s with processor %s after %d attempts", fileHandler.GetInputFile(), processor.Name(), currentNode.MaxRetries)
			e.sessionUpdatesChannel <- definitions.SessionUpdate{
				SessionID: sessionID,
				Finished:  true,
				Error:     newProcessorFailedError(currentNode.Name, err),
			}
		}
		return nil
	}

	nextProcessorNode, err := e.flowManager.GetNextProcessor(currentNode.FlowID, currentNode.FlowOrder)
	if err != nil {
		logger.WithError(err).Error("failed to find the next processor")
		return fmt.Errorf("%w: %v", ErrFailedToGetNextProcessor, err)
	}

	e.scheduleNextProcessor(sessionID, fileHandler, newFlow, nextProcessorNode, 0)
	return nil
}

func (e *Engine) createProcessorLogger(sessionID uuid.UUID, processor *models.Processor) (*logrus.Logger, error) {
	logDir := path.Join(e.config.Workdir,
		"logs",
		"flow="+processor.FlowID.String(),
		"sessions", sessionID.String(),
		processor.ID.String()+"_"+processor.Name+"_"+processor.Type+".log")
	err := utils.CreateDirsIfNotExist(logDir)
	if err != nil {
		return nil, err
	}

	logFilePath := path.Join(logDir, fmt.Sprintf("%d_%s_%s.log", processor.FlowOrder, processor.Name, processor.ID.String()))

	logger := logrus.New()
	file, err := utils.OpenFile(logFilePath)
	if err != nil {
		return nil, err
	}
	logger.SetOutput(file)
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(processor.LogLevel)

	return logger, nil
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
