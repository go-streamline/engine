package engine

import (
	"github.com/go-streamline/core/config"
	"github.com/go-streamline/core/definitions"
	"github.com/go-streamline/core/utils"
)

// DeepCopyWrapper meant for mocks
type DeepCopyWrapper struct{}

func (d DeepCopyWrapper) DeepCopyFlowObject(input *definitions.EngineFlowObject) (*definitions.EngineFlowObject, error) {
	return utils.DeepCopy(input)
}

func (d DeepCopyWrapper) DeepCopyConfig(input *config.Config) (*config.Config, error) {
	return utils.DeepCopy(input)
}

var DeepCopier = DeepCopyWrapper{}
