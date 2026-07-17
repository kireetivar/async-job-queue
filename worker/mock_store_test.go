package worker

import (
	"context"

	"github.com/kireetivar/async-job-queue/models"
	"github.com/kireetivar/async-job-queue/store"
)

// Ensure MockStore implements store.Store
var _ store.Store = (*MockStore)(nil)

type MockStore struct {
	// Add fields here to track calls or return custom mock values
	// e.g. EnqueueFunc func(ctx context.Context, job *models.Job) error
	EnqueueCalled bool
	EnqueuedJob   *models.Job
	EnqueueError  error

	DequeueCalled bool
	DequeuedJob   *models.Job
	DequeueError  error

	AckCalled bool
	AckError  error

	MoveToDeadLetterCalled bool
	MoveToDeadLetterJob    *models.Job
	MoveToDeadLetterError  error

	ScheduleDueCalled bool
	ScheduledJobs     []*models.Job
	ScheduleDueError  error

	GetJobCalled bool
	GetJobJobID  string
	GetJobError  error

	CancelJobCalled bool
	CancelJobJobID  string
	CancelJobError  error

	ListQueuesCalled bool
	Queues           []models.QueueInfo
	ListQueuesError  error

	PauseQueueCalled bool
	PausedQueueName  string
	PauseQueueError  error

	ResumeQueueCalled bool
	ResumedQueueName  string
	ResumeQueueError  error

	GetQueueStatusCalled bool
	QueueStats           []models.QueueStats
	GetQueueStatusError  error

	IsQueuePausedCalled bool
	IsQueuePausedName   string
	IsQueuePausedbool   bool
	IsQueuePausedError  error

	DequeueFunc       func(ctx context.Context, queues []string) (*models.Job, error)
	AckFunc           func(ctx context.Context, jobId string) error
	IsQueuePausedFunc func(ctx context.Context, name string) (bool, error)
}

func (m *MockStore) Enqueue(ctx context.Context, job *models.Job) error {
	m.EnqueueCalled = true
	m.EnqueuedJob = job
	return m.EnqueueError
}

func (m *MockStore) Dequeue(ctx context.Context, queues []string) (*models.Job, error) {
	m.DequeueCalled = true
	if m.DequeueFunc != nil {
		return m.DequeueFunc(ctx, queues)
	}
	return m.DequeuedJob, m.DequeueError
}

func (m *MockStore) Ack(ctx context.Context, jobId string) error {
	m.AckCalled = true
	if m.AckFunc != nil {
		return m.AckFunc(ctx, jobId)
	}
	return m.AckError
}

func (m *MockStore) MoveToDeadLetter(ctx context.Context, job *models.Job) error {
	m.MoveToDeadLetterCalled = true
	m.MoveToDeadLetterJob = job
	return m.MoveToDeadLetterError
}

func (m *MockStore) ScheduleDue(ctx context.Context) ([]*models.Job, error) {
	return nil, nil
}

func (m *MockStore) GetJob(ctx context.Context, jobID string) (*models.Job, error) {
	return nil, nil
}

func (m *MockStore) CancelJob(ctx context.Context, jobID string) error {
	return nil
}

func (m *MockStore) ListQueues(ctx context.Context) ([]models.QueueInfo, error) {
	return nil, nil
}

func (m *MockStore) PauseQueue(ctx context.Context, name string) error {
	return nil
}

func (m *MockStore) ResumeQueue(ctx context.Context, name string) error {
	return nil
}

func (m *MockStore) GetQueueStatus(ctx context.Context) ([]models.QueueStats, error) {
	return nil, nil
}

func (m *MockStore) IsQueuePaused(ctx context.Context, name string) (bool, error) {
	m.IsQueuePausedCalled = true
	if m.IsQueuePausedFunc != nil {
		return m.IsQueuePausedFunc(ctx, name)
	}
	return m.IsQueuePausedbool, m.IsQueuePausedError
}

func (m *MockStore) Ping(ctx context.Context) error {
    return nil
}
