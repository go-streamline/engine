package errors

import (
	"errors"
	"fmt"
)

var ErrProcessorTypeNotFound = errors.New("processor type not found")

type ProcessorTypeNotFound struct {
	Type string
}

func (e *ProcessorTypeNotFound) Error() string {
	return fmt.Sprintf("processor type %s not found", e.Type)
}

func (e *ProcessorTypeNotFound) Is(target error) bool {
	return target == ErrProcessorTypeNotFound
}

func NewProcessorTypeNotFoundError(t string) error {
	return &ProcessorTypeNotFound{
		Type: t,
	}
}
