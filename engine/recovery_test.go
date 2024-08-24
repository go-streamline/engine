package engine

import (
	"github.com/go-streamline/core/definitions"
	"github.com/go-streamline/core/engine/enginetests"
	"github.com/go-streamline/core/repo"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRecover_Success(t *testing.T) {
	mockFileHandler := new(enginetests.MockFileHandler)
	mockWAL := new(enginetests.MockWriteAheadLogger)

	engine := &Engine{
		writeAheadLogger:      mockWAL,
		log:                   logrus.New(),
		sessionUpdatesChannel: make(chan definitions.SessionUpdate, 1),
		retryQueue:            make(chan retryTask, 1),
		ignoreRecoveryErrors:  false,
	}

	sessionID := uuid.New()
	processorID := "testProcessor"
	entry := repo.LogEntry{
		SessionID:     sessionID,
		ProcessorName: "ProcessorName",
		ProcessorID:   processorID,
		InputFile:     "inputFile",
		OutputFile:    "outputFile",
	}

	mockWAL.On("ReadEntries").Return([]repo.LogEntry{entry}, nil)

	err := engine.Recover()
	assert.NoError(t, err)

	select {
	case task := <-engine.retryQueue:
		assert.Equal(t, processorID, task.processorID)
		assert.Equal(t, sessionID, task.sessionID)
	default:
		t.Error("Expected retry task to be scheduled, but got none")
	}

	mockWAL.AssertExpectations(t)
	mockFileHandler.AssertExpectations(t)
}
