package store

import (
	"context"

	"github.com/kireetivar/async-job-queue/models"
)

type Store interface {
	Enqueue(ctx context.Context, job *models.Job) error
	Dequeue(ctx context.Context, queues []string) (*models.Job, error)
	Ack(ctx context.Context, jobId string) error
	Nack(ctx context.Context, job *models.Job) error
	MoveToDeadLetter(ctx context.Context, job *models.Job) error
	ScheduleDue(ctx context.Context) ([]*models.Job, error)
	GetJob(ctx context.Context, jobID string) (*models.Job, error)
	CancelJob(ctx context.Context, jobID string) error
	ListQueues(ctx context.Context) ([]models.QueueInfo, error)
	PauseQueue(ctx context.Context, name string) error
	ResumeQueue(ctx context.Context, name string) error
	GetQueueStatus(ctx context.Context) ([]models.QueueStats, error)
	IsQueuePaused(ctx context.Context, name string) (bool, error)
}
