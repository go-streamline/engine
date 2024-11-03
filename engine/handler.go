package engine

import (
	"fmt"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/go-streamline/interfaces/utils"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"path"
)

var ErrCouldNotDeepCopyFlowObject = fmt.Errorf("could not deep copy flow object")
var ErrFailedToGetNextProcessor = fmt.Errorf("failed to get next processor")
var ErrFailedToSetProcessorConfig = fmt.Errorf("failed to set processor configuration")

type logrusProcessorHook struct {
	FlowID        uuid.UUID
	SessionID     uuid.UUID
	ProcessorID   uuid.UUID
	ProcessorName string
}

func (h *logrusProcessorHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (h *logrusProcessorHook) Fire(entry *logrus.Entry) error {
	entry.Data["flow_id"] = h.FlowID
	entry.Data["session_id"] = h.SessionID
	entry.Data["processor_id"] = h.ProcessorID
	entry.Data["processor_name"] = h.ProcessorName
	return nil
}

func (e *Engine) executeProcessor(flow *definitions.EngineFlowObject, fileHandler definitions.EngineFileHandler, sessionID uuid.UUID, attempts int, currentNode *definitions.SimpleProcessor) error {
	if !currentNode.Enabled {
		e.log.Infof("Skipping disabled processor %s in flow %s", currentNode.Name, currentNode.FlowID)
		return e.scheduleNextEnabledProcessor(sessionID, flow, fileHandler, currentNode)
	}

	processor, ok := e.enabledProcessors[currentNode.ID]
	if !ok {
		return fmt.Errorf("processor %s not found in enabled processors cache", currentNode.ID)
	}

	logEntry := definitions.LogEntry{
		SessionID:             sessionID,
		ProcessorName:         currentNode.Name,
		ProcessorID:           currentNode.ID.String(),
		InputFile:             fileHandler.GetInputFile(),
		OutputFile:            fileHandler.GetOutputFile(),
		FlowObject:            *flow,
		CompletedProcessorIDs: e.branchTracker.GetCompletedProcessors(sessionID),
		IsComplete:            false,
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

	e.writeAheadLogger.WriteEntry(definitions.LogEntry{
		SessionID:             sessionID,
		ProcessorName:         currentNode.Name,
		ProcessorID:           currentNode.ID.String(),
		InputFile:             fileHandler.GetInputFile(),
		OutputFile:            fileHandler.GetOutputFile(),
		FlowObject:            *flow,
		CompletedProcessorIDs: e.branchTracker.GetCompletedProcessors(sessionID),
		IsComplete:            true,
	})

	// After processor execution, schedule the next step
	return e.scheduleNextEnabledProcessor(sessionID, newFlow, fileHandler, currentNode)
}

func (e *Engine) scheduleNextEnabledProcessor(
	sessionID uuid.UUID,
	flow *definitions.EngineFlowObject,
	fileHandler definitions.EngineFileHandler,
	currentNode *definitions.SimpleProcessor,
) error {
	nextProcessorNodes, err := e.flowManager.GetProcessors(currentNode.NextProcessorIDs)
	if err != nil {
		e.log.WithError(err).Errorf("failed to find the next processor for %s", currentNode.ID)
		return fmt.Errorf("%w: %v", ErrFailedToGetNextProcessor, err)
	}

	for _, nextNode := range nextProcessorNodes {
		if !nextNode.Enabled {
			e.log.Infof("Skipping disabled processor %s in flow %s", nextNode.Name, nextNode.FlowID)
			continue
		}

		newFileHandler, err := fileHandler.GenerateNewFileHandler()
		if err != nil {
			e.log.WithError(err).Errorf("failed to create file handler for processor %s", nextNode.Name)
			continue
		}

		// Add the processor to the branch tracker with its dependencies
		e.branchTracker.AddProcessor(sessionID, nextNode.ID, nextNode.NextProcessorIDs)

		// Schedule the processor for execution
		e.scheduleNextProcessor(sessionID, newFileHandler, flow, &nextNode, 0)
	}

	// Mark the current processor as complete
	allComplete := e.branchTracker.MarkComplete(sessionID, currentNode.ID)
	if allComplete && len(currentNode.NextProcessorIDs) == 0 {
		// If all processors in the current branch are done and there are no more processors, mark the flow as complete
		e.sessionUpdatesChannel <- definitions.SessionUpdate{
			SessionID: sessionID,
			Finished:  true,
			Error:     nil,
		}
	} else if allComplete {
		// If all processors in this branch have completed, fetch the next processors from the flow manager and schedule them
		nextProcessors, err := e.flowManager.GetProcessors(currentNode.NextProcessorIDs)
		if err != nil {
			e.log.WithError(err).Errorf("failed to get next processors for %s", currentNode.ID)
			return fmt.Errorf("%w: %v", ErrFailedToGetNextProcessor, err)
		}

		for _, nextProc := range nextProcessors {
			if !nextProc.Enabled {
				continue
			}

			newFileHandler, err := fileHandler.GenerateNewFileHandler()
			if err != nil {
				e.log.WithError(err).Errorf("failed to create file handler for processor %s", nextProc.Name)
				continue
			}

			// Schedule the next processor in the sequence
			e.scheduleNextProcessor(sessionID, newFileHandler, flow, &nextProc, 0)
		}
	}

	return nil
}

func (e *Engine) scheduleNextProcessor(
	sessionID uuid.UUID,
	fileHandler definitions.EngineFileHandler,
	flow *definitions.EngineFlowObject,
	currentNode *definitions.SimpleProcessor,
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

	e.workerPool.Submit(func() {
		e.processJob(processingJob{
			sessionID:   sessionID,
			attempts:    attempts,
			flow:        flow,
			fileHandler: fileHandler,
			currentNode: currentNode,
		})
	})
}

func (e *Engine) createProcessorLogger(sessionID uuid.UUID, processor *definitions.SimpleProcessor) (*logrus.Logger, error) {
	logDir := path.Join(e.config.Workdir,
		"logs",
		"flow="+processor.FlowID.String(),
		"sessions", sessionID.String(),
		processor.ID.String()+"_"+processor.Name+"_"+processor.Type+".log")
	err := utils.CreateDirsIfNotExist(logDir)
	if err != nil {
		return nil, err
	}

	logFilePath := path.Join(logDir, fmt.Sprintf("%s_%s.log", processor.Name, processor.ID.String()))

	logger := logrus.New()
	file, err := utils.OpenFile(logFilePath)
	if err != nil {
		return nil, err
	}
	logger.SetOutput(io.MultiWriter(file, os.Stdout))
	logger.AddHook(&logrusProcessorHook{FlowID: processor.FlowID, SessionID: sessionID, ProcessorID: processor.ID, ProcessorName: processor.Name})
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(processor.LogLevel)

	return logger, nil
}
