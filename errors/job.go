package errors

import "fmt"

var FailedToCreateFile = fmt.Errorf("failed to create initial job file")
var FailedToCopyJobToContentsFolder = fmt.Errorf("failed to copy job to contents folder")
var FailedToExecuteProcessors = fmt.Errorf("failed to execute processors")
