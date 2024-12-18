package engine

import (
	"errors"
	"fmt"
	"github.com/go-streamline/core/filehandler"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/go-streamline/interfaces/utils"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"path"
	"time"
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

	newFlow, err := e.safelyExecuteProcessor(processor, copiedFlow, fileHandler, logger)
	isError := e.handleProcessorFailure(flow, fileHandler, sessionID, attempts, currentNode, err, logger, processor)
	if isError {
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

func (e *Engine) safelyExecuteProcessor(
	processor definitions.Processor,
	flowObject *definitions.EngineFlowObject,
	fileHandler definitions.EngineFileHandler,
	logger *logrus.Logger,
) (result *definitions.EngineFlowObject, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("panic occurred in processor %s, will yield for %d seconds before executing retry mechanism: %v", processor.Name(), e.config.ProcessorPanicYieldSeconds, r)
			result = nil
			err = fmt.Errorf("%w: %v", ErrProcessorPanicked, r)
		}
	}()

	return processor.Execute(flowObject, fileHandler, logger)
}

func (e *Engine) handleProcessorFailure(
	flow *definitions.EngineFlowObject,
	fileHandler definitions.EngineFileHandler,
	sessionID uuid.UUID,
	attempts int,
	currentNode *definitions.SimpleProcessor,
	err error,
	logger *logrus.Logger,
	processor definitions.Processor,
) bool {
	if err != nil {
		if attempts < currentNode.MaxRetries {
			newHandler := filehandler.NewCopyOnWriteEngineFileHandler(fileHandler.GetInputFile())
			go func() {
				if errors.Is(err, ErrProcessorPanicked) {
					time.Sleep(time.Duration(e.config.ProcessorPanicYieldSeconds) * time.Second)
				}
				logger.WithError(err).Warnf("Processor %s failed, will attempt retry (%d/%d) in %d seconds", processor.Name(), attempts+1, currentNode.MaxRetries, currentNode.BackoffSeconds)
				time.Sleep(time.Duration(currentNode.BackoffSeconds) * time.Second)
				e.scheduleNextProcessor(sessionID, newHandler, flow, currentNode, attempts+1)
			}()
		} else {
			logger.WithError(err).Errorf("failed to handle %s with processor %s after %d attempts", fileHandler.GetInputFile(), processor.Name(), currentNode.MaxRetries)
			e.sessionUpdatesChannel <- definitions.SessionUpdate{
				SessionID: sessionID,
				Finished:  true,
				TPMark:    flow.TPMark,
				Error:     newProcessorFailedError(currentNode.Name, err),
			}
		}
		return true
	}
	return false
}

func (e *Engine) scheduleNextEnabledProcessor(
	sessionID uuid.UUID,
	flow *definitions.EngineFlowObject,
	fileHandler definitions.EngineFileHandler,
	currentNode *definitions.SimpleProcessor,
) error {
	nextProcessorNodes := currentNode.NextProcessors

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
		e.branchTracker.AddProcessor(sessionID, nextNode.ID, e.getProcessorsIDs(nextNode.NextProcessors))

		// Schedule the processor for execution
		e.scheduleNextProcessor(sessionID, newFileHandler, flow, nextNode, 0)
	}

	// Mark the current processor as complete
	allComplete := e.branchTracker.MarkComplete(sessionID, currentNode.ID)
	if allComplete && len(currentNode.NextProcessors) == 0 {
		// If all processors in the current branch are done and there are no more processors, mark the flow as complete
		e.sessionUpdatesChannel <- definitions.SessionUpdate{
			SessionID: sessionID,
			Finished:  true,
			Error:     nil,
			TPMark:    flow.TPMark,
		}
	} else if allComplete {
		// If all processors in this branch have completed, fetch the next processors from the flow manager and schedule them
		for _, nextProc := range currentNode.NextProcessors {
			if !nextProc.Enabled {
				continue
			}

			newFileHandler, err := fileHandler.GenerateNewFileHandler()
			if err != nil {
				e.log.WithError(err).Errorf("failed to create file handler for processor %s", nextProc.Name)
				continue
			}

			// Schedule the next processor in the sequence
			e.scheduleNextProcessor(sessionID, newFileHandler, flow, nextProc, 0)
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
			TPMark:    flow.TPMark,
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
