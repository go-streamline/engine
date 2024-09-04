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
	paginationRequest := &definitions.PaginationRequest{
		Page:    1,
		PerPage: e.config.FlowBatchSize,
	}

	for {
		select {
		case <-e.ctx.Done():
			e.log.Infof("stopping flow monitoring")
			return
		case <-ticker.C:
			lastQueryTime = e.monitorAndTrackFlows(paginationRequest, lastQueryTime)
		}
	}
}

// monitorAndTrackFlows fetches flows from the flow manager, stores them in memory,
// and activates or deactivates flows accordingly.
func (e *Engine) monitorAndTrackFlows(paginationRequest *definitions.PaginationRequest, lastQueryTime time.Time) time.Time {
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
	if _, ok := e.activeFlows[flow.ID]; ok {
		return nil
	}
	e.activeFlows[flow.ID] = flow
	triggerProcessors, err := e.flowManager.GetTriggerProcessorsForFlow(flow.ID)
	if err != nil {
		e.log.WithError(err).Errorf("failed to get trigger processors for flow %s", flow.ID)
		return err
	}

	for _, triggerProcessorDef := range triggerProcessors {
		triggerProcessor, err := e.processorFactory.GetTriggerProcessor(triggerProcessorDef.Type)
		if err != nil {
			e.log.WithError(err).Errorf("failed to get processor %s for flow %s", triggerProcessorDef.Type, flow.ID)
			return err
		}

		e.triggerProcessors[triggerProcessorDef.ID] = triggerProcessor
		err = triggerProcessor.SetConfig(triggerProcessorDef.Config)
		if err != nil {
			e.log.WithError(err).Errorf("failed to set configuration for trigger processor %s in flow %s", triggerProcessorDef.Name, flow.ID)
			return err
		}
		go e.runTriggerProcessor(triggerProcessor, triggerProcessorDef, flow)
	}

	return nil
}

func (e *Engine) deactivateFlow(flowID uuid.UUID) {
	if flow, ok := e.activeFlows[flowID]; ok {
		for _, triggerProcessorDef := range flow.TriggerProcessors {
			if tp, ok := e.triggerProcessors[triggerProcessorDef.ID]; ok {
				tp.Close()
				delete(e.triggerProcessors, triggerProcessorDef.ID)
			}
		}
		delete(e.activeFlows, flowID)
	}
}
