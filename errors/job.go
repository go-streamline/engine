package errors

import "fmt"

var FailedToCreateFile = fmt.Errorf("failed to create initial job file")
var FailedToCopyJobToContentsFolder = fmt.Errorf("failed to copy job to contents folder")
var FailedToExecuteProcessors = fmt.Errorf("failed to execute processors")
var NoProcessorsAvailable = fmt.Errorf("no processors available")
var FailedToSetProcessorConfig = fmt.Errorf("failed to set processor configuration")
var ErrFailedToCreateLogger = fmt.Errorf("failed to create logger")

type FailedToCreateLogger struct {
	ProcessorName string
	Err           error
}

func (e *FailedToCreateLogger) Error() string {
	return fmt.Sprintf("failed to create logger for processor %s: %v", e.ProcessorName, e.Err)
}

func (e *FailedToCreateLogger) Unwrap() error {
	return e.Err
}

func (e *FailedToCreateLogger) Is(target error) bool {
	return target == ErrFailedToCreateLogger
}

func NewFailedToCreateLoggerError(processorName string, err error) error {
	return &FailedToCreateLogger{
		ProcessorName: processorName,
		Err:           fmt.Errorf("%w", err),
	}
}
