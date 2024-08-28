package engine

import (
	"github.com/go-streamline/core/filehandler"
	"github.com/go-streamline/core/models"
	"github.com/go-streamline/core/repo"
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
		fileHandler := filehandler.NewEngineFileHandler(lastEntry.InputFile)
		flow := &lastEntry.FlowObject
		currentNode, err := e.getProcessorByID(lastEntry.FlowID, uuid.MustParse(lastEntry.ProcessorID))
		if err != nil {
			e.log.WithError(err).Warnf("Failed to find processor with ID %s during recovery", lastEntry.ProcessorID)
			continue
		}

		e.scheduleNextProcessor(sessionID, fileHandler, flow, currentNode, lastEntry.RetryCount)
	}

	log.Info("go-streamline recovery process complete")
	return nil
}

func (e *Engine) getProcessorByID(flowID uuid.UUID, id uuid.UUID) (*models.Processor, error) {
	processor, err := e.flowManager.GetProcessorByID(flowID, id)
	if err != nil {
		return nil, newProcessorNotFoundError(id)
	}
	return processor, nil
}

func (e *Engine) createSessionIDToLastEntryMap(entries []repo.LogEntry) map[uuid.UUID]repo.LogEntry {
	sessionMap := make(map[uuid.UUID]repo.LogEntry)

	for _, entry := range entries {
		// if the session is marked as ended, remove it from the session map
		if entry.ProcessorName == "__end__" {
			delete(sessionMap, entry.SessionID)
			continue
		}
		sessionMap[entry.SessionID] = entry
	}
	return sessionMap
}
