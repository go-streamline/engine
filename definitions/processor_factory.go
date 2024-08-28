package definitions

import (
	"errors"
	"fmt"
	"reflect"
)

var ErrProcessorTypeNotFound = errors.New("processor type not found")

type processorTypeNotFound struct {
	Type string
}

func (e *processorTypeNotFound) Error() string {
	return fmt.Sprintf("processor type %s not found", e.Type)
}

func (e *processorTypeNotFound) Is(target error) bool {
	return target == ErrProcessorTypeNotFound
}

func newProcessorTypeNotFoundError(t string) error {
	return &processorTypeNotFound{
		Type: t,
	}
}

// ProcessorFactory defines an interface for retrieving processors.
type ProcessorFactory interface {
	GetProcessor(typeName string) (Processor, error)
	RegisterProcessor(processor Processor)
}

// DefaultProcessorFactory is an implementation of ProcessorFactory.
type DefaultProcessorFactory struct {
	processorMap map[string]reflect.Type
}

// NewDefaultProcessorFactory creates a new DefaultProcessorFactory.
func NewDefaultProcessorFactory() *DefaultProcessorFactory {
	return &DefaultProcessorFactory{
		processorMap: make(map[string]reflect.Type),
	}
}

// RegisterProcessor registers a processor type with the factory.
func (f *DefaultProcessorFactory) RegisterProcessor(processor Processor) {
	typeName := f.getTypeName(processor)
	f.processorMap[typeName] = reflect.TypeOf(processor).Elem()
}

func (f *DefaultProcessorFactory) GetProcessor(typeName string) (Processor, error) {
	processorType, exists := f.processorMap[typeName]
	if !exists {
		return nil, newProcessorTypeNotFoundError(typeName)
	}

	processorInstance := reflect.New(processorType).Interface().(Processor)
	return processorInstance, nil
}

// getTypeName returns the fully qualified type name of a processor.
func (f *DefaultProcessorFactory) getTypeName(processor Processor) string {
	processorType := reflect.TypeOf(processor).Elem()
	return fmt.Sprintf("%s.%s", processorType.PkgPath(), processorType.Name())
}
