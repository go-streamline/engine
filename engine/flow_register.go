package engine

import (
	"errors"
	"fmt"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/google/uuid"
	"time"
)

var ErrFuncPanicked = fmt.Errorf("function panicked")

func (e *Engine) monitorFlows() {
	ticker := time.NewTicker(time.Duration(e.config.FlowCheckInterval) * time.Second)
	defer ticker.Stop()

	lastQueryTime := e.monitorAndTrackFlows(time.Time{})

	for {
		select {
		case <-e.ctx.Done():
			e.log.Infof("stopping flow monitoring")
			e.workerPool.Stop()
			return
		case <-ticker.C:
			lastQueryTime = e.monitorAndTrackFlows(lastQueryTime)
		}
	}
}

func (e *Engine) monitorAndTrackFlows(lastQueryTime time.Time) time.Time {
	paginationRequest := &definitions.PaginationRequest{
		Page:    1,
		PerPage: e.config.FlowBatchSize,
	}
	totalProcessed := 0
	timeNow := time.Now()

	for {
		paginatedFlows, err := e.flowManager.ListFlows(paginationRequest, lastQueryTime)
		if err != nil {
			e.log.WithError(err).Errorf("could not fetch flows: page %d", paginationRequest.Page)
			break
		}

		for _, flow := range paginatedFlows.Data {
			if flow.Active {
				err = e.activateFlow(flow)
				if err != nil {
					e.log.WithError(err).Errorf("failed to activate flow %s", flow.ID)
					return lastQueryTime
				}
			} else {
				err = e.deactivateFlow(flow.ID)
				if err != nil {
					e.log.WithError(err).Errorf("failed to deactivate flow %s", flow.ID)
				}
			}
		}

		totalProcessed += len(paginatedFlows.Data)

		if totalProcessed >= paginatedFlows.TotalCount {
			break
		}

		paginationRequest.Page++
	}

	return timeNow
}

func (e *Engine) activateFlow(flow *definitions.Flow) error {
	// Activate and cache flow if not already active
	if _, ok := e.activeFlows[flow.ID]; ok {
		return nil
	}
	e.activeFlows[flow.ID] = flow

	triggerProcessors, err := e.flowManager.GetTriggerProcessorsForFlow(flow.ID)
	if err != nil {
		e.log.WithError(err).Errorf("failed to get trigger processors for flow %s", flow.ID)
		return err
	}

	activatedTriggerProcessors := 0
	var allErrors error

	for _, triggerProcessorDef := range triggerProcessors {
		if triggerProcessorDef.Enabled {
			triggerProcessor, err := e.processorFactory.GetTriggerProcessor(triggerProcessorDef.ID, triggerProcessorDef.Type)
			if err != nil {
				allErrors = errors.Join(allErrors, err)
				e.log.WithError(err).Errorf("failed to get trigger processor %s for flow %s", triggerProcessorDef.Name, flow.ID)
				continue
			}
			tpInfo := triggerProcessorInfo{
				// Initialize processors if enabled
				Processor:  triggerProcessor,
				FlowID:     flow.ID,
				Definition: triggerProcessorDef,
			}
			e.triggerProcessors[triggerProcessorDef.ID] = tpInfo
			err = e.safelyRunErrorFunc(func() error {
				return triggerProcessor.SetConfig(triggerProcessorDef.Config)
			})
			if err != nil {
				allErrors = errors.Join(allErrors, err)
				e.log.WithError(err).Errorf("failed to set configuration for trigger processor %s in flow %s", triggerProcessorDef.Name, flow.ID)
				continue
			}
			if triggerProcessor.GetScheduleType() == definitions.EventDriven {
				go e.runTriggerProcessor(triggerProcessor, triggerProcessorDef, flow)
			} else {
				scheduleID, err := e.scheduler.AddFunc(triggerProcessorDef.CronExpr, func() {
					e.runTriggerProcessor(triggerProcessor, triggerProcessorDef, flow)
				})
				if err != nil {
					allErrors = errors.Join(allErrors, err)
					e.log.WithError(err).Errorf("failed to schedule cron for trigger processor %s in flow %s", triggerProcessorDef.Name, flow.ID)
					continue
				}
				tpInfo.CronEntryID = scheduleID
			}
			activatedTriggerProcessors++
		}
	}

	if activatedTriggerProcessors == 0 {
		e.log.Warnf("no trigger processors activated for flow %s", flow.ID)
		defer e.deactivateFlow(flow.ID)
		return allErrors
	}
	for _, processor := range flow.Processors {
		if processor.Enabled {
			impl, err := e.processorFactory.GetProcessor(processor.ID, processor.Type)
			if err != nil {
				e.log.WithError(err).Errorf("failed to get processor %s for flow %s", processor.Name, flow.ID)
				continue
			}
			err = e.safelyRunErrorFunc(func() error {
				return impl.SetConfig(processor.Config)
			})
			if err != nil {
				e.log.WithError(err).Errorf("failed to set configuration for processor %s in flow %s", processor.Name, flow.ID)
				defer e.deactivateFlow(flow.ID)
				return err
			}
			e.enabledProcessors[processor.ID] = impl
		}
	}

	return nil
}

func (e *Engine) safelyRunErrorFunc(f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			e.log.Errorf("panic while running function: %v", r)
			err = fmt.Errorf("%w: %v", ErrFuncPanicked, r)
		}
	}()
	return f()
}

func (e *Engine) deactivateFlow(flowID uuid.UUID) error {
	var allErrors error
	if flow, ok := e.activeFlows[flowID]; ok {
		// deactivate trigger processors
		for _, triggerProcessorDef := range flow.TriggerProcessors {
			if tp, ok := e.triggerProcessors[triggerProcessorDef.ID]; ok {
				if tp.CronEntryID != 0 {
					e.scheduler.Remove(tp.CronEntryID)
				}

				err := e.safelyRunErrorFunc(func() error {
					return tp.Processor.Close()
				})
				if err != nil {
					e.log.WithError(err).Errorf("failed to close trigger processor %s[id=%s] in flow %s",
						tp.Processor.Name(), triggerProcessorDef.ID, flowID)
					allErrors = errors.Join(allErrors, err)
				}
				tp.Definition.Enabled = false
				delete(e.triggerProcessors, triggerProcessorDef.ID)
			}
		}

		// deactivate regular processors
		for _, processor := range flow.Processors {
			if processor.Enabled {
				err := e.safelyRunErrorFunc(func() error {
					return e.enabledProcessors[processor.ID].Close()
				})
				if err != nil {
					e.log.WithError(err).Errorf("failed to close processor %s[id=%s] in flow %s", processor.Name,
						processor.ID, flowID)
					allErrors = errors.Join(allErrors, err)
				}
				delete(e.enabledProcessors, processor.ID)
			}
			delete(e.enabledProcessors, processor.ID)
		}

		delete(e.activeFlows, flowID)
	}

	return allErrors
}
