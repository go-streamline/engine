package engine

import (
	"github.com/go-streamline/engine/configuration"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/go-streamline/interfaces/utils"
)

// DeepCopyWrapper meant for mocks
type DeepCopyWrapper struct{}

func (d DeepCopyWrapper) DeepCopyFlowObject(input *definitions.EngineFlowObject) (*definitions.EngineFlowObject, error) {
	return utils.DeepCopy(input)
}

func (d DeepCopyWrapper) DeepCopyConfig(input *configuration.Config) (*configuration.Config, error) {
	return utils.DeepCopy(input)
}

var DeepCopier = DeepCopyWrapper{}
