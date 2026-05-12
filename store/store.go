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
}
