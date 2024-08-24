package engine

import (
	"errors"
	"github.com/go-streamline/core/config"
	"github.com/go-streamline/core/definitions"
	"github.com/go-streamline/core/engine/enginetests"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

func TestExecuteProcessors_Success(t *testing.T) {
	mockProcessor := new(enginetests.MockProcessor)
	mockFileHandler := new(enginetests.MockFileHandler)
	mockWAL := new(enginetests.MockWriteAheadLogger)

	engine := &Engine{
		writeAheadLogger:      mockWAL,
		log:                   logrus.New(),
		sessionUpdatesChannel: make(chan definitions.SessionUpdate, 1),
		Processors:            []config.ProcessorConfig{{Processor: mockProcessor}},
	}

	sessionID := uuid.New()
	processorID := "testProcessor"
	flow := &definitions.EngineFlowObject{}

	mockProcessor.On("GetID").Return(processorID)
	mockProcessor.On("Name").Return("ProcessorName")
	mockProcessor.On("Execute", flow, mockFileHandler).Return(flow, nil)
	mockWAL.On("WriteEntry", mock.Anything).Return()

	mockFileHandler.On("GetInputFile").Return("inputFile")
	mockFileHandler.On("GetOutputFile").Return("outputFile")
	mockFileHandler.On("GenerateNewFileHandler").Return(mockFileHandler, nil)

	err := engine.executeProcessors(flow, mockFileHandler, "", sessionID)
	assert.NoError(t, err)

	select {
	case update := <-engine.sessionUpdatesChannel:
		assert.Equal(t, sessionID, update.SessionID)
		assert.True(t, update.Finished)
		assert.NoError(t, update.Error)
	default:
		t.Error("Expected a session update, but got none")
	}

	mockProcessor.AssertExpectations(t)
	mockWAL.AssertExpectations(t)
	mockFileHandler.AssertExpectations(t)
}

func TestExecuteProcessors_ProcessorFailure(t *testing.T) {
	// Mock dependencies
	mockProcessor := new(enginetests.MockProcessor)
	mockFileHandler := new(enginetests.MockFileHandler)
	mockWAL := new(enginetests.MockWriteAheadLogger)

	// Override the DeepCopier with a mock implementation if necessary
	originalDeepCopier := DeepCopier
	defer func() { DeepCopier = originalDeepCopier }() // Restore the original after test

	DeepCopier = DeepCopyWrapper{} // Using the default, you can replace it with a mock if you need to simulate DeepCopy failures

	// Create the engine instance
	testEngine := &Engine{
		writeAheadLogger:      mockWAL,
		log:                   logrus.New(),
		sessionUpdatesChannel: make(chan definitions.SessionUpdate, 1),
		retryQueue:            make(chan retryTask, 1),
		Processors: []config.ProcessorConfig{
			{
				Processor: mockProcessor,
				Retry:     config.ProcessorRetryMechanism{BackOffInterval: time.Millisecond * 1},
			},
		},
	}

	sessionID := uuid.New()
	processorID := "testProcessor"
	flow := &definitions.EngineFlowObject{}

	// Mock method expectations
	mockProcessor.On("GetID").Return(processorID)
	mockProcessor.On("Name").Return("ProcessorName")
	mockProcessor.On("Execute", flow, mockFileHandler).Return(nil, errors.New("processor error"))
	mockWAL.On("WriteEntry", mock.Anything).Return(nil)

	mockFileHandler.On("GetInputFile").Return("inputFile")
	mockFileHandler.On("GetOutputFile").Return("outputFile")

	// Run the method under test
	err := testEngine.executeProcessors(flow, mockFileHandler, "", sessionID)
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	// Check that the retry task was scheduled
	select {
	case task := <-testEngine.retryQueue:
		assert.Equal(t, sessionID, task.sessionID)
		assert.Equal(t, processorID, task.processorID)
		assert.Equal(t, 1, task.attempts)
	default:
		t.Error("Expected a retry task to be scheduled, but got none")
	}

	// Assert that all expectations were met
	mockProcessor.AssertExpectations(t)
	mockWAL.AssertExpectations(t)
	mockFileHandler.AssertExpectations(t)
}
