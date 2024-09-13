package engine

import (
	"context"
	"fmt"
	"github.com/go-streamline/core/filehandler"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"path"
)

var ErrFailedToExecuteProcessors = fmt.Errorf("failed to execute processors")

type processingJob struct {
	sessionID   uuid.UUID
	attempts    int
	flow        *definitions.EngineFlowObject
	fileHandler definitions.EngineFileHandler
	currentNode *definitions.SimpleProcessor
}

type triggerProcessorInfo struct {
	Processor   definitions.TriggerProcessor
	FlowID      uuid.UUID
	Definition  *definitions.SimpleTriggerProcessor
	CronEntryID cron.EntryID
}

type scheduler interface {
	AddFunc(spec string, cmd func()) (cron.EntryID, error)
	Stop() context.Context
	Start()
	Remove(id cron.EntryID)
}

type workerPool interface {
	StopAndWait()
	Submit(task func())
	Stop() context.Context
}

func (e *Engine) processJob(job processingJob) {
	err := e.executeProcessor(job.flow, job.fileHandler, job.sessionID, job.attempts, job.currentNode)
	if err != nil {
		e.log.WithError(err).Errorf("failed to execute processor for session %s", job.sessionID)
		e.sessionUpdatesChannel <- definitions.SessionUpdate{
			SessionID: job.sessionID,
			Finished:  true,
			Error:     fmt.Errorf("%w: %v", ErrFailedToExecuteProcessors, err),
		}
	}
}

func (e *Engine) runTriggerProcessor(tp definitions.TriggerProcessor, triggerProcessorDef *definitions.SimpleTriggerProcessor, flow *definitions.Flow) {
	defer func() {
		err := tp.Close()
		if err != nil {
			e.log.WithError(err).Errorf("failed to close trigger processor %s for flow %s", tp.Name(), flow.ID)
		}
	}()

	if triggerProcessorDef.ScheduleType == definitions.EventDriven {
		// event-driven trigger processor uses the loop
		for triggerProcessorDef.Enabled {
			select {
			case <-e.ctx.Done():
				return
			default:
				e.executeTriggerProcessor(tp, triggerProcessorDef, flow)
			}
		}
	} else if triggerProcessorDef.ScheduleType == definitions.CronDriven {
		// cron-driven trigger processors are executed by the scheduler without loop
		e.executeTriggerProcessor(tp, triggerProcessorDef, flow)
	}
}

func (e *Engine) executeTriggerProcessor(tp definitions.TriggerProcessor, triggerProcessorDef *definitions.SimpleTriggerProcessor, flow *definitions.Flow) {
	// generate a new session id for this execution of the flow
	sessionID := uuid.New()

	// create a new file handler for the trigger processor's output
	outputFile := path.Join(e.config.Workdir, "contents", uuid.New().String())
	fileHandler := filehandler.NewWriteOnlyEngineFileHandler(outputFile)

	// create the flow object that the processor will use
	flowObject := &definitions.EngineFlowObject{
		Metadata: map[string]interface{}{},
	}

	// execute the trigger processor
	_, err := tp.Execute(flowObject, fileHandler, e.log)
	if err != nil {
		e.log.WithError(err).Errorf("failed to execute trigger processor %s in flow %s", triggerProcessorDef.Name, flow.ID)
		return
	}

	// get the first set of processors for the flow
	initialProcessors, err := e.flowManager.GetFirstProcessorsForFlow(flow.ID)
	if err != nil {
		e.log.WithError(err).Errorf("failed to get first processors for flow %s", flow.ID)
		return
	}

	// add the initial processors to the branch tracker and schedule them
	for _, processor := range initialProcessors {
		if !processor.Enabled {
			e.log.Infof("skipping disabled processor %s in flow %s", processor.Name, flow.ID)
			continue
		}

		// add the processor to the branch tracker with its next processors
		e.branchTracker.AddProcessor(sessionID, processor.ID, processor.NextProcessorIDs)

		// generate a new file handler for each processor's output
		newFileHandler, err := fileHandler.GenerateNewFileHandler()
		if err != nil {
			e.log.WithError(err).Errorf("failed to create file handler for processor %s", processor.Name)
			continue
		}

		e.scheduleNextProcessor(sessionID, newFileHandler, flowObject, &processor, 0)
	}

	// for event-driven trigger processors, wait for all processors to complete
	if triggerProcessorDef.ScheduleType == definitions.EventDriven {
		for !e.branchTracker.IsComplete(sessionID) {
			select {
			case <-e.ctx.Done():
				return
			}
		}
	}
}
