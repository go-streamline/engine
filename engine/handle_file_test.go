package engine

import (
	"errors"
	"github.com/go-streamline/core/definitions"
	"github.com/go-streamline/core/engine/enginetests"
	"github.com/go-streamline/core/repo"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHandleFile_Success(t *testing.T) {
	mockWAL := new(enginetests.MockWriteAheadLogger)
	mockFileHandler := new(enginetests.MockFileHandler)
	mockUtils := new(MockUtils)

	engine := &Engine{
		writeAheadLogger:      mockWAL,
		log:                   logrus.New(),
		sessionUpdatesChannel: make(chan definitions.SessionUpdate, 1),
		contentsDir:           "testDir",
	}

	sessionID := uuid.New()
	filePath := "inputFile"
	walEntry := repo.LogEntry{
		SessionID:     sessionID,
		ProcessorName: "__init__",
		ProcessorID:   "__init__",
		InputFile:     filePath,
		OutputFile:    "testDir/outputFile",
		FlowObject:    definitions.EngineFlowObject{},
	}

	mockWAL.On("WriteEntry", walEntry).Return()
	mockUtils.On("CopyFile", filePath, "testDir/outputFile").Return(nil)
	mockFileHandler.On("GetInputFile").Return("testDir/outputFile")
	mockFileHandler.On("GetOutputFile").Return("testDir/outputFile")
	mockFileHandler.On("GenerateNewFileHandler").Return(mockFileHandler, nil)

	engine.handleFile(definitions.EngineIncomingObject{Reader: filePath})

	select {
	case update := <-engine.sessionUpdatesChannel:
		assert.Equal(t, sessionID, update.SessionID)
		assert.True(t, update.Finished)
		assert.NoError(t, update.Error)
	default:
		t.Error("Expected a session update, but got none")
	}

	mockWAL.AssertExpectations(t)
	mockUtils.AssertExpectations(t)
	mockFileHandler.AssertExpectations(t)
}

func TestHandleFile_CopyFileError(t *testing.T) {
	mockWAL := new(enginetests.MockWriteAheadLogger)
	mockUtils := new(MockUtils)

	engine := &Engine{
		writeAheadLogger:      mockWAL,
		log:                   logrus.New(),
		sessionUpdatesChannel: make(chan definitions.SessionUpdate, 1),
		contentsDir:           "testDir",
		retryQueue:            make(chan retryTask, 1),
	}

	sessionID := uuid.New()
	filePath := "inputFile"
	walEntry := repo.LogEntry{
		SessionID:     sessionID,
		ProcessorName: "__init__",
		ProcessorID:   "__init__",
		InputFile:     filePath,
		OutputFile:    "",
		FlowObject:    definitions.EngineFlowObject{},
	}

	mockWAL.On("WriteEntry", walEntry).Return()
	mockUtils.On("CopyFile", filePath, "testDir/outputFile").Return(errors.New("copy error"))

	engine.handleFile(definitions.EngineIncomingObject{Reader: filePath})

	select {
	case task := <-engine.retryQueue:
		assert.Equal(t, sessionID, task.sessionID)
		assert.Equal(t, "__init__", task.processorID)
		assert.Equal(t, 0, task.attempts)
	default:
		t.Error("Expected a retry task to be scheduled, but got none")
	}

	mockWAL.AssertExpectations(t)
	mockUtils.AssertExpectations(t)
}
