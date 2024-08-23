package engine

import (
	"fmt"
	"github.com/go-streamline/core/utils"
	"testing"

	"github.com/go-streamline/core/definitions"
	"github.com/go-streamline/core/repo"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockUtils struct {
	mock.Mock
}

func (m *MockUtils) copyFileNoError(src, dst string) error {
	return nil
}

func (m *MockUtils) copyFileError(src, dst string) error {
	return fmt.Errorf("copy error")
}

func TestGetProcessHandlerForSession(t *testing.T) {
	mockUtils := new(MockUtils)
	originalCopyFile := utils.CopyFile
	utils.CopyFile = mockUtils.copyFileNoError
	defer func() { utils.CopyFile = originalCopyFile }()

	sessionID := uuid.New()

	t.Run("Recovery from __init__ state", func(t *testing.T) {
		utils.CopyFile = mockUtils.copyFileNoError

		entry := repo.LogEntry{
			SessionID:  sessionID,
			HandlerID:  "__init__",
			InputFile:  "input/path/file.txt",
			OutputFile: "output/path/file.txt",
			FlowObject: definitions.EngineFlowObject{Metadata: map[string]interface{}{"key": "value"}},
		}

		engine := Engine{}

		fileHandler, flow, err := engine.getProcessHandlerForSession(sessionID, entry)
		assert.NoError(t, err)
		assert.Contains(t, fileHandler.GetOutputFile(), "output/path/")
		assert.Equal(t, entry.FlowObject, *flow)
	})

	t.Run("Recovery from __init__ with copy file error", func(t *testing.T) {
		utils.CopyFile = mockUtils.copyFileError

		entry := repo.LogEntry{
			SessionID:  sessionID,
			HandlerID:  "__init__",
			InputFile:  "input/path/file.txt",
			OutputFile: "output/path/file.txt",
			FlowObject: definitions.EngineFlowObject{Metadata: map[string]interface{}{"key": "value"}},
		}

		engine := Engine{}

		fileHandler, flow, err := engine.getProcessHandlerForSession(sessionID, entry)
		assert.Error(t, err)
		assert.Nil(t, fileHandler)
		assert.Nil(t, flow)
	})

	t.Run("Recovery from handler state", func(t *testing.T) {
		utils.CopyFile = mockUtils.copyFileNoError
		entry := repo.LogEntry{
			SessionID:  sessionID,
			HandlerID:  "handler1",
			InputFile:  "input/path/file.txt",
			OutputFile: "output/path/file.txt",
			FlowObject: definitions.EngineFlowObject{Metadata: map[string]interface{}{"key": "value"}},
		}

		engine := Engine{}

		fileHandler, flow, err := engine.getProcessHandlerForSession(sessionID, entry)
		assert.NoError(t, err)
		assert.Equal(t, "input/path/file.txt", fileHandler.GetInputFile())
		assert.Equal(t, entry.FlowObject, *flow)
	})
}
