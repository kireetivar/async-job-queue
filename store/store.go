package store

import (
	"context"

	"github.com/kireetivar/async-job-queue/models"
)

type Store interface {
	Enqueue(ctx context.Context, job *models.Job) error
	Dequeue(ctx context.Context, queues []string) (*models.Job, error)
	Ack(ctx context.Context, JobId string) error
}
