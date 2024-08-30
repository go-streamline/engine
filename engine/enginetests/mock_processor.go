package enginetests

import (
	"github.com/go-streamline/interfaces/definitions"
	"github.com/stretchr/testify/mock"
)

type MockProcessor struct {
	mock.Mock
}

func (m *MockProcessor) Execute(info *definitions.EngineFlowObject, fileHandler definitions.ProcessorFileHandler) (*definitions.EngineFlowObject, error) {
	args := m.Called(info, fileHandler)
	arg0 := args.Get(0)
	if arg0 == nil {
		return nil, args.Error(1)
	} else {
		return arg0.(*definitions.EngineFlowObject), args.Error(1)
	}
}

func (m *MockProcessor) GetID() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockProcessor) Name() string {
	args := m.Called()
	return args.String(0)
}
