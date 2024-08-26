package models

import (
	"github.com/google/uuid"
)

// Processor represents the processor configuration stored in the database.
type Processor struct {
	ID         uuid.UUID `gorm:"type:uuid;primaryKey"`
	FlowID     uuid.UUID `gorm:"column:flow_id;type:uuid;not null"`
	Name       string    `gorm:"type:varchar(255);not null"`
	Type       string    `gorm:"type:varchar(255);not null"`
	FlowOrder  int       `gorm:"column:flow_order;not null"`
	MaxRetries int       `gorm:"column:max_retries;default:0"`
}

func (p *Processor) TableName() string {
	return "processors"
}
