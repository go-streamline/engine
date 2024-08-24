package engine

import (
	"github.com/go-streamline/core/definitions"
	"github.com/go-streamline/core/filehandler"
	"github.com/go-streamline/core/repo"
	"github.com/go-streamline/core/utils"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

func (e *Engine) recover() error {
	log.Debugf("go-streamline is recovering from WriteAheadLogger")
	entries, err := e.writeAheadLogger.ReadEntries()
	if err != nil {
		return err
	}
	log.Debugf("read %d entries from WriteAheadLogger", len(entries))

	if entries == nil || len(entries) == 0 {
		log.Info("no entries found in WriteAheadLogger; nothing to recover.")
		return nil
	}

	sessionMap := e.createSessionIDToLastEntryMap(entries)

	for sessionID, lastEntry := range sessionMap {
		fileHandler, flow, err := e.getProcessHandlerForSession(sessionID, lastEntry)
		if err != nil {
			if !e.ignoreRecoveryErrors {
				return err
			}
			continue
		}

		e.retryQueue <- retryTask{
			flow:        flow,
			fileHandler: fileHandler,
			processorID: lastEntry.ProcessorID,
			sessionID:   sessionID,
			attempts:    lastEntry.RetryCount,
		}
	}

	log.Info("go-streamline recovery process complete")
	return nil
}

func (e *Engine) createSessionIDToLastEntryMap(entries []repo.LogEntry) map[uuid.UUID]repo.LogEntry {
	sessionMap := make(map[uuid.UUID]repo.LogEntry)

	for _, entry := range entries {
		// If the session is marked as ended, remove it from the session map
		if entry.ProcessorName == "__end__" {
			delete(sessionMap, entry.SessionID)
			continue
		}
		sessionMap[entry.SessionID] = entry
	}
	return sessionMap
}

func (e *Engine) getProcessHandlerForSession(sessionID uuid.UUID, lastEntry repo.LogEntry) (definitions.EngineFileHandler, *definitions.EngineFlowObject, error) {
	log.Debugf("go-streamline is recovering session %s starting from handler %s", sessionID, lastEntry.ProcessorID)
	var fileHandler definitions.EngineFileHandler

	if lastEntry.ProcessorID == "__init__" {
		log.Debugf("last entry for session %s was '__init__'", sessionID)
		err := utils.CopyFile(lastEntry.InputFile, lastEntry.OutputFile)
		if err != nil {
			log.WithError(err).Errorf("failed to recover during __init__ CopyFile operation from %s to %s", lastEntry.InputFile, lastEntry.OutputFile)
			return nil, nil, err
		}
		fileHandler = filehandler.NewEngineFileHandler(lastEntry.OutputFile)
	} else {
		fileHandler = filehandler.NewEngineFileHandler(lastEntry.InputFile)
	}

	flow := lastEntry.FlowObject
	return fileHandler, &flow, nil
}
