package engine

import (
	"context"
	"errors"
	"fmt"
	"github.com/alitto/pond/v2"
	"github.com/go-streamline/core/filehandler"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"path"
	"time"
)

var ErrFailedToExecuteProcessors = fmt.Errorf("failed to execute processors")
var ErrProcessorPanicked = fmt.Errorf("processor panicked")

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
	Submit(task func()) pond.Task
	Stop() pond.Task
}

func (e *Engine) processJob(job processingJob) {
	err := e.executeProcessor(job.flow, job.fileHandler, job.sessionID, job.attempts, job.currentNode)
	if err != nil {
		e.log.WithError(err).Errorf("failed to execute processor for session %s", job.sessionID)
		e.sessionUpdatesChannel <- definitions.SessionUpdate{
			SessionID: job.sessionID,
			Finished:  true,
			TPMark:    job.flow.TPMark,
			Error:     fmt.Errorf("%w: %v", ErrFailedToExecuteProcessors, err),
		}
	}
}

func (e *Engine) runTriggerProcessor(tp definitions.TriggerProcessor, triggerProcessorDef *definitions.SimpleTriggerProcessor, flow *definitions.Flow) {
	if tp.GetScheduleType() == definitions.EventDriven {
		defer func() {
			err := e.safelyRunErrorFunc(func() error {
				return tp.Close()
			})
			if err != nil {
				e.log.WithError(err).Errorf("failed to close trigger processor %s for flow %s", tp.Name(), flow.ID)
			}
		}()
		// event-driven trigger processor uses the loop
		for triggerProcessorDef.Enabled {
			select {
			case <-e.ctx.Done():
				return
			default:
				if e.coordinator != nil && triggerProcessorDef.SingleNode {
					isLeader, err := e.coordinator.IsLeader(triggerProcessorDef.ID)
					if err != nil {
						e.log.WithError(err).Errorf("failed to check if trigger processor %s is leader", triggerProcessorDef.Name)
						continue
					}

					if !isLeader {
						time.Sleep(100 * time.Millisecond)
						continue
					}

					logrus.Debugf("trigger processor %s is leader", triggerProcessorDef.Name)
				}
				e.executeTriggerProcessor(tp, triggerProcessorDef, flow)
			}
		}
	} else if tp.GetScheduleType() == definitions.CronDriven {
		// cron-driven trigger processors are executed by the scheduler without loop
		e.executeTriggerProcessor(tp, triggerProcessorDef, flow)
	}
}

func (e *Engine) executeTriggerProcessor(tp definitions.TriggerProcessor, triggerProcessorDef *definitions.SimpleTriggerProcessor, flow *definitions.Flow) {
	// create the flow object that the processor will use
	flowObject := &definitions.EngineFlowObject{
		Metadata: map[string]interface{}{},
	}

	// execute the trigger processor
	responses, err := e.safelyExecuteTriggerProcessor(tp, flowObject, func() definitions.ProcessorFileHandler {
		outputFile := path.Join(e.config.Workdir, "contents", uuid.NewString())
		return filehandler.NewWriteOnlyEngineFileHandler(outputFile)
	}, e.log)
	if err != nil {
		if errors.Is(err, ErrProcessorPanicked) && tp.GetScheduleType() == definitions.EventDriven {
			e.log.WithError(err).Errorf(
				"trigger processor %s panicked in flow %s, will yield for %d seconds",
				triggerProcessorDef.Name,
				flow.ID,
				e.config.TriggerProcessorPanicYieldSeconds,
			)
			time.Sleep(time.Duration(e.config.TriggerProcessorPanicYieldSeconds) * time.Second)
			return
		} else {
			e.log.WithError(err).Errorf("failed to execute trigger processor %s in flow %s", triggerProcessorDef.Name, flow.ID)
			return
		}
	}

	// get the first set of processors for the flow
	initialProcessors, err := e.flowManager.GetFirstProcessorsForFlow(flow.ID)
	if err != nil {
		e.log.WithError(err).Errorf("failed to get first processors for flow %s", flow.ID)
		return
	}

	for _, response := range responses {
		// generate a new session id for this execution of the flow
		sessionID := uuid.New()
		// add the initial processors to the branch tracker and schedule them
		for _, processor := range initialProcessors {
			if !processor.Enabled {
				e.log.Infof("skipping disabled processor %s in flow %s", processor.Name, flow.ID)
				continue
			}

			// add the processor to the branch tracker with its next processors
			e.branchTracker.AddProcessor(sessionID, processor.ID, processor.NextProcessorIDs)
			// generate a new file handler for each processor's output
			newFileHandler, err := response.FileHandler.(definitions.EngineFileHandler).GenerateNewFileHandler()
			if err != nil {
				e.log.WithError(err).Errorf("failed to create file handler for processor %s", processor.Name)
				continue
			}

			e.scheduleNextProcessor(sessionID, newFileHandler, response.EngineFlowObject, processor, 0)
		}
		// for event-driven trigger processors, wait for all processors to complete
		if tp.GetScheduleType() == definitions.EventDriven {
			for !e.branchTracker.IsComplete(sessionID) {
				select {
				case <-e.ctx.Done():
					return
				}
			}
		}
	}

}

func (e *Engine) safelyExecuteTriggerProcessor(
	tp definitions.TriggerProcessor,
	info *definitions.EngineFlowObject,
	produceFileHandler func() definitions.ProcessorFileHandler,
	log *logrus.Logger,
) (resp []*definitions.TriggerProcessorResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			resp = nil
			err = fmt.Errorf("%w: %v", ErrProcessorPanicked, r)
		}
	}()

	return tp.Execute(info, produceFileHandler, log)
}
