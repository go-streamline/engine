package definitions

import (
	"github.com/go-streamline/core/utils"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/sirupsen/logrus"
	"io"
)

type EngineFlowObject struct {
	Metadata map[string]interface{} `json:"metadata"`
}

type EngineIncomingObject struct {
	FlowID    uuid.UUID
	Metadata  map[string]interface{}
	Reader    io.Reader
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
	Execute(info *EngineFlowObject, fileHandler ProcessorFileHandler, log *logrus.Logger) (*EngineFlowObject, error)
	SetConfig(config map[string]interface{}) error
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
