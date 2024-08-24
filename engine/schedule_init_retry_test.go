package engine

import (
	"github.com/go-streamline/core/config"
	"github.com/go-streamline/core/definitions"
	"github.com/go-streamline/core/engine/enginetests"
	"github.com/go-streamline/core/repo"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestScheduleInitRetry(t *testing.T) {
	mockWAL := new(enginetests.MockWriteAheadLogger)
	engine := &Engine{
		writeAheadLogger: mockWAL,
		log:              logrus.New(),
		retryQueue:       make(chan retryTask, 1),
		config: &config.Config{
			InitRetryBackOff: 1 * time.Millisecond,
		},
	}

	sessionID := uuid.New()
	filePath := "inputFile"
	i := definitions.EngineIncomingObject{Filepath: filePath}
	walEntry := repo.LogEntry{
		SessionID:     sessionID,
		ProcessorName: "__init__",
		ProcessorID:   "__init__",
		InputFile:     filePath,
		OutputFile:    "",
		FlowObject:    transformIncomingObjectToFlowObject(i),
		RetryCount:    0,
	}

	mockWAL.On("WriteEntry", walEntry).Return()

	engine.scheduleInitRetry(i, sessionID)
	time.Sleep(10 * time.Millisecond)

	select {
	case task := <-engine.retryQueue:
		assert.Equal(t, sessionID, task.sessionID)
		assert.Equal(t, "__init__", task.processorID)
		assert.Equal(t, 0, task.attempts)
		assert.Equal(t, filePath, task.fileHandler.GetInputFile())
	default:
		t.Error("Expected a retry task to be scheduled, but got none")
	}

	mockWAL.AssertExpectations(t)
}
