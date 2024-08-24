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

func (m *MockUtils) copyFile(src, dst string) error {
	args := m.Called(src, dst)
	return args.Error(0)
}

func TestGetProcessHandlerForSession(t *testing.T) {
	mockUtils := new(MockUtils)
	originalCopyFile := utils.CopyFile
	utils.CopyFile = mockUtils.copyFile
	defer func() { utils.CopyFile = originalCopyFile }()

	sessionID := uuid.New()

	t.Run("Recovery from __init__ state", func(t *testing.T) {
		file := "input/path/file.txt"
		outputFile := "output/path/file.txt"
		mockUtils.On("copyFile", file, outputFile).Return(nil)

		entry := repo.LogEntry{
			SessionID:   sessionID,
			ProcessorID: "__init__",
			InputFile:   file,
			OutputFile:  outputFile,
			FlowObject:  definitions.EngineFlowObject{Metadata: map[string]interface{}{"key": "value"}},
		}

		engine := Engine{}

		fileHandler, flow, err := engine.getProcessHandlerForSession(sessionID, entry)
		assert.NoError(t, err)
		assert.Contains(t, fileHandler.GetOutputFile(), "output/path/")
		assert.Equal(t, entry.FlowObject, *flow)
	})

	t.Run("Recovery from __init__ with copy file error", func(t *testing.T) {
		inputFile := "input/path/error.txt"
		output := "output/path/error.txt"
		mockUtils.On("copyFile", inputFile, output).Return(fmt.Errorf("copy error"))
		utils.CopyFile = mockUtils.copyFile

		entry := repo.LogEntry{
			SessionID:   sessionID,
			ProcessorID: "__init__",
			InputFile:   inputFile,
			OutputFile:  output,
			FlowObject:  definitions.EngineFlowObject{Metadata: map[string]interface{}{"key": "value"}},
		}

		engine := Engine{}

		fileHandler, flow, err := engine.getProcessHandlerForSession(sessionID, entry)
		assert.Error(t, err)
		assert.Nil(t, fileHandler)
		assert.Nil(t, flow)
	})

	t.Run("Recovery from handler state", func(t *testing.T) {
		mockUtils.On("copyFile", mock.Anything, mock.Anything).Return(nil)
		entry := repo.LogEntry{
			SessionID:   sessionID,
			ProcessorID: "handler1",
			InputFile:   "input/path/file.txt",
			OutputFile:  "output/path/file.txt",
			FlowObject:  definitions.EngineFlowObject{Metadata: map[string]interface{}{"key": "value"}},
		}

		engine := Engine{}

		fileHandler, flow, err := engine.getProcessHandlerForSession(sessionID, entry)
		assert.NoError(t, err)
		assert.Equal(t, "input/path/file.txt", fileHandler.GetInputFile())
		assert.Equal(t, entry.FlowObject, *flow)
	})
}
