package errors

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
)

var ErrProcessorsNotFound = errors.New("processor not found")

type ProcessorNotFoundError struct {
	ID uuid.UUID
}

func (e *ProcessorNotFoundError) Error() string {
	return fmt.Sprintf("processor %s not found", e.ID)
}

func (e *ProcessorNotFoundError) Is(target error) bool {
	return target == ErrProcessorsNotFound
}

func NewProcessorNotFoundError(id uuid.UUID) error {
	return &ProcessorNotFoundError{
		ID: id,
	}
}
