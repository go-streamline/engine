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
	Metadata       map[string]interface{}
	Filepath       string
	StartHandlerID string
	SessionID      uuid.UUID
}

func (e *EngineFlowObject) EvaluateExpression(input string) (string, error) {
	return utils.EvaluateExpression(input, e.Metadata)
}

type BaseHandler struct {
	ID string
}

func (b *BaseHandler) GetID() string {
	return b.ID
}

func (b *BaseHandler) DecodeMap(input interface{}, output interface{}) error {
	return mapstructure.Decode(input, output)
}

type Handler interface {
	GetID() string
	Name() string
	Handle(info *EngineFlowObject, fileHandler HandlerFileHandler) (*EngineFlowObject, error)
}

type HandlerFileHandler interface {
	Read() (io.Reader, error)
	Write() (io.Writer, error)
}

type EngineFileHandler interface {
	HandlerFileHandler
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
