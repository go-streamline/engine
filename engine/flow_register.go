package engine

import (
	"github.com/go-streamline/interfaces/definitions"
	"github.com/google/uuid"
	"time"
)

func (e *Engine) monitorFlows() {
	ticker := time.NewTicker(time.Duration(e.config.FlowCheckInterval) * time.Second)
	defer ticker.Stop()

	var lastQueryTime time.Time

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
				e.deactivateFlow(flow.ID)
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

	// Initialize processors if enabled
	for _, processor := range flow.Processors {
		if processor.Enabled {
			impl, err := e.processorFactory.GetProcessor(processor.ID, processor.Type)
			if err != nil {
				e.log.WithError(err).Errorf("failed to get processor %s for flow %s", processor.Name, flow.ID)
				continue
			}
			e.enabledProcessors[processor.ID] = impl
		}
	}

	for _, triggerProcessorDef := range triggerProcessors {
		if triggerProcessorDef.Enabled {
			triggerProcessor, err := e.processorFactory.GetTriggerProcessor(triggerProcessorDef.ID, triggerProcessorDef.Type)
			if err != nil {
				e.log.WithError(err).Errorf("failed to get trigger processor %s for flow %s", triggerProcessorDef.Name, flow.ID)
				continue
			}
			tpInfo := triggerProcessorInfo{
				Processor:  triggerProcessor,
				FlowID:     flow.ID,
				Definition: triggerProcessorDef,
			}
			e.triggerProcessors[triggerProcessorDef.ID] = tpInfo
			err = triggerProcessor.SetConfig(triggerProcessorDef.Config)
			if err != nil {
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
					e.log.WithError(err).Errorf("failed to schedule cron for trigger processor %s in flow %s", triggerProcessorDef.Name, flow.ID)
					continue
				}
				tpInfo.CronEntryID = scheduleID
			}
		}
	}

	return nil
}

func (e *Engine) deactivateFlow(flowID uuid.UUID) {
	if flow, ok := e.activeFlows[flowID]; ok {
		// deactivate trigger processors
		for _, triggerProcessorDef := range flow.TriggerProcessors {
			if tp, ok := e.triggerProcessors[triggerProcessorDef.ID]; ok {
				if tp.CronEntryID != 0 {
					e.scheduler.Remove(tp.CronEntryID)
				}

				err := tp.Processor.Close()
				if err != nil {
					e.log.WithError(err).Errorf("failed to close trigger processor %s[id=%s] in flow %s",
						tp.Processor.Name(), triggerProcessorDef.ID, flowID)
				}
				tp.Definition.Enabled = false
				delete(e.triggerProcessors, triggerProcessorDef.ID)
			}
		}

		// deactivate regular processors
		for _, processor := range flow.Processors {
			if processor.Enabled {
				err := e.enabledProcessors[processor.ID].Close()
				if err != nil {
					e.log.WithError(err).Errorf("failed to close processor %s[id=%s] in flow %s", processor.Name,
						processor.ID, flowID)
				}
				delete(e.enabledProcessors, processor.ID)
			}
			delete(e.enabledProcessors, processor.ID)
		}

		delete(e.activeFlows, flowID)
	}
}
