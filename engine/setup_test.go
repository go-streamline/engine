package engine

import (
	"context"
	"github.com/alitto/pond/v2"
	coremocks "github.com/go-streamline/core/mocks"
	"github.com/go-streamline/engine/configuration"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
)

type MockWorkerPool struct {
	mock.Mock
}

func (m *MockWorkerPool) Submit(task func()) pond.Task {
	args := m.Called(task)
	return args.Get(0).(pond.Task)
}

func (m *MockWorkerPool) StopAndWait() {
	m.Called()
}

func (m *MockWorkerPool) Stop() pond.Task {
	args := m.Called()
	return args.Get(0).(pond.Task)
}

type MockScheduler struct {
	mock.Mock
}

func (m *MockScheduler) AddFunc(spec string, cmd func()) (cron.EntryID, error) {
	args := m.Called(spec, cmd)
	return args.Get(0).(cron.EntryID), args.Error(1)
}

func (m *MockScheduler) Stop() context.Context {
	args := m.Called()
	return args.Get(0).(context.Context)
}

func (m *MockScheduler) Start() {
	m.Called()
}

func (m *MockScheduler) Remove(id cron.EntryID) {
	m.Called(id)
}

type TestSuite struct {
	flowManager      *coremocks.MockFlowManager
	wal              *coremocks.MockWriteAheadLogger
	processorFactory *coremocks.MockProcessorFactory
	workerPool       *MockWorkerPool
	branchTracker    *coremocks.MockBranchTracker
	scheduler        *MockScheduler
}

func setupConfig() *configuration.Config {
	return &configuration.Config{
		Workdir:           "/tmp/engine_tests",
		MaxWorkers:        10,
		FlowBatchSize:     5,
		FlowCheckInterval: 5,
	}
}

func setupTestEngine(config *configuration.Config) (*Engine, *TestSuite) {
	mockFlowManager := new(coremocks.MockFlowManager)
	mockWAL := new(coremocks.MockWriteAheadLogger)
	mockProcessorFactory := new(coremocks.MockProcessorFactory)
	mockWorkerPool := new(MockWorkerPool)
	mockBranchTracker := new(coremocks.MockBranchTracker)
	mockScheduler := new(MockScheduler)
	logger := logrus.New()

	// Return the engine instance
	return &Engine{
			config:                config,
			sessionUpdatesChannel: make(chan definitions.SessionUpdate),
			writeAheadLogger:      mockWAL,
			workerPool:            mockWorkerPool,
			log:                   logger,
			processorFactory:      mockProcessorFactory,
			flowManager:           mockFlowManager,
			branchTracker:         mockBranchTracker,
			activeFlows:           make(map[uuid.UUID]*definitions.Flow),
			enabledProcessors:     make(map[uuid.UUID]definitions.Processor),
			triggerProcessors:     make(map[uuid.UUID]triggerProcessorInfo),
			scheduler:             mockScheduler,
		}, &TestSuite{
			flowManager:      mockFlowManager,
			wal:              mockWAL,
			processorFactory: mockProcessorFactory,
			workerPool:       mockWorkerPool,
			branchTracker:    mockBranchTracker,
			scheduler:        mockScheduler,
		}
}
