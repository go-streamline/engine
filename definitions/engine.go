package definitions

import (
	"github.com/go-streamline/core/utils"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"io"
)

type EngineFlowObject struct {
	Metadata map[string]interface{} `json:"metadata"`
}

type EngineIncomingObject struct {
	Metadata  map[string]interface{}
	Filepath  string
	SessionID uuid.UUID
}

func (e *EngineFlowObject) EvaluateExpression(input string) (string, error) {
	return utils.EvaluateExpression(input, e.Metadata)
}

type BaseProcessor struct {
	ID string
}

func (b *BaseProcessor) GetID() string {
	return b.ID
}

func (b *BaseProcessor) DecodeMap(input interface{}, output interface{}) error {
	return mapstructure.Decode(input, output)
}

type Processor interface {
	GetID() string
	Name() string
	Execute(info *EngineFlowObject, fileHandler ProcessorFileHandler) (*EngineFlowObject, error)
}

type ProcessorFileHandler interface {
	Read() (io.Reader, error)
	Write() (io.Writer, error)
}

type EngineFileHandler interface {
	ProcessorFileHandler
	GetInputFile() string
	GetOutputFile() string
	Close()
	GenerateNewFileHandler() (EngineFileHandler, error)
}

type SessionUpdate struct {
	SessionID uuid.UUID
	Finished  bool
	Error     error
}
