package errors

import "fmt"

var CouldNotDeepCopyFlowObject = fmt.Errorf("could not deep copy flow object")
var ErrProcessorFailed = fmt.Errorf("processor failed")
var FailedToGetNextProcessor = fmt.Errorf("failed to get next processor")
var CouldNotCreateFlowManager = fmt.Errorf("could not create flow manager")

type ProcessorFailed struct {
	ProcessorName string
	Err           error
}

func (e *ProcessorFailed) Error() string {
	return fmt.Sprintf("processor %s failed: %v", e.ProcessorName, e.Err)
}

func (e *ProcessorFailed) Unwrap() error {
	return e.Err
}

func (e *ProcessorFailed) Is(target error) bool {
	return target == ErrProcessorFailed
}

func NewProcessorFailedError(processorName string, err error) error {
	return &ProcessorFailed{
		ProcessorName: processorName,
		Err:           fmt.Errorf("%w", err),
	}
}
