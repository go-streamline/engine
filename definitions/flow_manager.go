package definitions

import (
	"github.com/go-streamline/core/models"
	"github.com/google/uuid"
)

type FlowManager interface {
	GetFirstProcessorForFlow(flowID uuid.UUID) (*models.Processor, error)
	ListFlows() ([]models.Flow, error)
	GetFlowByID(flowID uuid.UUID) (*models.Flow, error)
	GetProcessorByID(flowID uuid.UUID, processorID uuid.UUID) (*models.Processor, error)
	GetNextProcessor(flowID uuid.UUID, currentOrder int) (*models.Processor, error)
}
