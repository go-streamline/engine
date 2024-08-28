package engine

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
)

var ErrProcessorsNotFound = errors.New("processor not found")
var ErrProcessorFailed = fmt.Errorf("processor failed")
var ErrFailedToCreateLogger = fmt.Errorf("failed to create logger")

type failedToCreateLogger struct {
	ProcessorName string
	Err           error
}

type processorFailedError struct {
	ProcessorName string
	Err           error
}

type processorNotFoundError struct {
	ID uuid.UUID
}

func (e *failedToCreateLogger) Error() string {
	return fmt.Sprintf("failed to create logger for processor %s: %v", e.ProcessorName, e.Err)
}

func (e *failedToCreateLogger) Unwrap() error {
	return e.Err
}

func (e *failedToCreateLogger) Is(target error) bool {
	return target == ErrFailedToCreateLogger
}

func (e *processorFailedError) Error() string {
	return fmt.Sprintf("processor %s failed: %v", e.ProcessorName, e.Err)
}

func (e *processorFailedError) Unwrap() error {
	return e.Err
}

func (e *processorFailedError) Is(target error) bool {
	return target == ErrProcessorFailed
}

func (e *processorNotFoundError) Error() string {
	return fmt.Sprintf("processor %s not found", e.ID)
}

func (e *processorNotFoundError) Is(target error) bool {
	return target == ErrProcessorsNotFound
}

func newFailedToCreateLoggerError(processorName string, err error) error {
	return &failedToCreateLogger{
		ProcessorName: processorName,
		Err:           fmt.Errorf("%w", err),
	}
}

func newProcessorFailedError(processorName string, err error) error {
	return &processorFailedError{
		ProcessorName: processorName,
		Err:           fmt.Errorf("%w", err),
	}
}

func newProcessorNotFoundError(id uuid.UUID) error {
	return &processorNotFoundError{
		ID: id,
	}
}
