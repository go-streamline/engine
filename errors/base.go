package errors

import "fmt"

var CouldNotDeepCopyFlowObject = fmt.Errorf("could not deep copy flow object")
var FailedToGenerateNewFileHandler = fmt.Errorf("failed to generate new file handler")
var ProcessorNotFound = fmt.Errorf("failed to find processor")
var ProcessorFailed = fmt.Errorf("processor failed")
