package store_test

import (
	"context"
	"testing"

	"github.com/kireetivar/async-job-queue/models"
	"github.com/kireetivar/async-job-queue/store"
	"github.com/kireetivar/async-job-queue/testutil"
)

func TestEnqueueDequeue(t *testing.T) {
	rc := testutil.SetupRedis(t)

	rs := store.NewRedisStore(rc)

	job := models.Job{
		ID:       "job-1",
		Queue:    "test-queue",
		Type:     "test-task",
		Priority: 5,
	}

	ctx := context.Background()

	err := rs.Enqueue(ctx, &job)
	if err != nil {
		t.Fatalf("Enqueue Failed: %v", err)
	}

	dequeuedJob, err := rs.Dequeue(ctx, []string{"test-queue"})
	if err != nil {
		t.Fatalf("Dequeue Failed: %v", err)
	}

	if job.ID != dequeuedJob.ID {
		t.Errorf("Expected ID %s, got %s", job.ID, dequeuedJob.ID)
	}

	if job.Queue != dequeuedJob.Queue {
		t.Errorf("Expected Queue: %s, got %s", job.Queue, dequeuedJob.Queue)
	}

	if job.Type != dequeuedJob.Type {
		t.Errorf("Expected Type: %s, got %s", job.Type, dequeuedJob.Type)
	}

	if job.Priority != dequeuedJob.Priority {
		t.Errorf("Extected Priority: %d, got %d", job.Priority, dequeuedJob.Priority)
	}
}
