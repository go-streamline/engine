package engine

import (
	"github.com/go-streamline/core/filehandler"
	"github.com/google/uuid"
)

func (e *Engine) recover() error {
	e.log.Debugf("go-streamline is recovering from WriteAheadLogger")
	// read only the last active (incomplete) entries
	lastEntries, err := e.writeAheadLogger.ReadLastEntries()
	if err != nil {
		return err
	}
	e.log.Debugf("read %d last active entries from WriteAheadLogger", len(lastEntries))

	if len(lastEntries) == 0 {
		e.log.Info("no active entries found in WriteAheadLogger; nothing to recover.")
		return nil
	}

	for _, lastEntry := range lastEntries {
		// create the file handler for the last known input file
		fileHandler := filehandler.NewEngineFileHandler(lastEntry.InputFile)

		// retrieve the processor for the flow and session being recovered
		flowObject := lastEntry.FlowObject
		processor, err := e.flowManager.GetProcessorByID(lastEntry.FlowID, uuid.MustParse(lastEntry.ProcessorID))
		if err != nil {
			e.log.WithError(err).Warnf("failed to find processor with ID %s during recovery", lastEntry.ProcessorID)
			continue
		}

		// restore the state of the branch tracker to resume from where we left off
		e.branchTracker.RestoreState(lastEntry.SessionID, lastEntry.CompletedProcessorIDs)

		// re-schedule the processor using the flow object and file handler
		e.scheduleNextProcessor(lastEntry.SessionID, fileHandler, &flowObject, processor, lastEntry.RetryCount)
	}

	e.log.Info("go-streamline recovery process complete")
	return nil
}
