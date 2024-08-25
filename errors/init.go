package errors

import "fmt"

var CouldNotCreateDirs = fmt.Errorf("could not create directories")
var RecoveryError = fmt.Errorf("failed to recover, if you don't want to recover, please delete the WAL file or set IgnoreRecoveryErrors to true")
var CouldNotDeepCopyConfig = fmt.Errorf("could not deep copy config")
var CouldNotGetDBConnection = fmt.Errorf("could not get db connection")
var CouldNotRunMigrations = fmt.Errorf("could not run migrations")
