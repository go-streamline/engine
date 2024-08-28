package repo

import (
	"errors"
	"fmt"
	"github.com/go-streamline/core/definitions"
	"github.com/go-streamline/core/models"
	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"path"
)

var ErrCouldNotGetDBConnection = fmt.Errorf("could not get db connection")
var ErrCouldNotRunMigrations = fmt.Errorf("could not run migrations")

type DefaultFlowManager struct {
	db *gorm.DB
}

func NewDefaultFlowManager(db *gorm.DB, workDir string) (definitions.FlowManager, error) {
	db, err := runMigrations(db, workDir)
	if err != nil {
		return nil, err
	}
	return &DefaultFlowManager{db: db}, nil
}

func newSQLiteDB(workDir string) (*gorm.DB, error) {
	return gorm.Open(sqlite.Open(path.Join(workDir, "db", "go-streamline.db")), &gorm.Config{})
}

func runMigrations(db *gorm.DB, workDir string) (*gorm.DB, error) {
	var err error
	if db == nil {
		db, err = newSQLiteDB(workDir)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrCouldNotGetDBConnection, err)
		}
	}

	s, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrCouldNotGetDBConnection, err)
	}

	if err = goose.Up(s, "./migrations"); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrCouldNotRunMigrations, err)
	}

	return db, nil
}

func (fm *DefaultFlowManager) GetFirstProcessorForFlow(flowID uuid.UUID) (*models.Processor, error) {
	var processor models.Processor
	err := fm.db.Where("flow_id = ?", flowID).Order("flow_order asc").First(&processor).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &processor, nil
}

func (fm *DefaultFlowManager) ListFlows() ([]models.Flow, error) {
	var flows []models.Flow
	err := fm.db.Find(&flows).Error
	if err != nil {
		return nil, err
	}
	return flows, nil
}

func (fm *DefaultFlowManager) GetFlowByID(flowID uuid.UUID) (*models.Flow, error) {
	var flow models.Flow
	err := fm.db.First(&flow, "id = ?", flowID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &flow, nil
}

func (fm *DefaultFlowManager) GetProcessorByID(flowID uuid.UUID, processorID uuid.UUID) (*models.Processor, error) {
	var processor models.Processor
	err := fm.db.Where("flow_id = ? AND id = ?", flowID, processorID).First(&processor).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &processor, nil
}

func (fm *DefaultFlowManager) GetNextProcessor(flowID uuid.UUID, currentOrder int) (*models.Processor, error) {
	var nextProcessor models.Processor
	err := fm.db.Where("flow_id = ? AND flow_order > ?", flowID, currentOrder).
		Order("flow_order asc").
		First(&nextProcessor).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &nextProcessor, nil
}

func (fm *DefaultFlowManager) ListProcessorsForFlow(flowID uuid.UUID) ([]models.Processor, error) {
	var processors []models.Processor
	err := fm.db.Where("flow_id = ?", flowID).Order("flow_order asc").Find(&processors).Error
	if err != nil {
		return nil, err
	}
	return processors, nil
}

func (fm *DefaultFlowManager) AddProcessorToFlow(flowID uuid.UUID, processor *models.Processor) error {
	// validate processor order is unique
	var count int64
	err := fm.db.Model(&models.Processor{}).
		Where("flow_id = ? AND flow_order = ?", flowID, processor.FlowOrder).
		Count(&count).Error
	if err != nil {
		return err
	}
	processor.FlowID = flowID
	err = fm.db.Create(processor).Error
	if err != nil {
		return err
	}
	return nil
}
