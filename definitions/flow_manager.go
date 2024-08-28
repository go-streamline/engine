package definitions

import (
	"fmt"
	"github.com/go-streamline/core/models"
	"github.com/google/uuid"
)

var ErrProcessorOrderExists = fmt.Errorf("there's already a processor with the same order in the flow")

type FlowManager interface {
	GetFirstProcessorForFlow(flowID uuid.UUID) (*models.Processor, error)
	ListFlows() ([]models.Flow, error)
	GetFlowByID(flowID uuid.UUID) (*models.Flow, error)
	GetProcessorByID(flowID uuid.UUID, processorID uuid.UUID) (*models.Processor, error)
	GetNextProcessor(flowID uuid.UUID, currentOrder int) (*models.Processor, error)
	ListProcessorsForFlow(flowID uuid.UUID) ([]models.Processor, error)
	AddProcessorToFlow(flowID uuid.UUID, processor *models.Processor) error
}
