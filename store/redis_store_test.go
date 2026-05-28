package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/kireetivar/async-job-queue/models"
	"github.com/kireetivar/async-job-queue/store"
	"github.com/kireetivar/async-job-queue/testutil"
)

func TestEnqueueDequeue(t *testing.T) {
	rc := testutil.SetupRedis(t)
	rs := store.NewRedisStore(rc)
	ctx := context.Background()

	job := &models.Job{
		ID:       "job-1",
		Queue:    "test-queue",
		Type:     "test-task",
		Priority: 5,
	}

	if err := rs.Enqueue(ctx, job); err != nil {
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

func TestEnqueueDelayed(t *testing.T) {
	rc := testutil.SetupRedis(t)
	rs := store.NewRedisStore(rc)
	ctx := context.Background()

	futureTime := time.Now().Add(1 * time.Hour)
	job := &models.Job{
		ID:       "job-1",
		Queue:    "test-queue",
		Type:     "test-task",
		Priority: 5,
		RunAt:    &futureTime,
	}

	if err := rs.Enqueue(ctx, job); err != nil {
		t.Errorf("Enqueue Failed: %v", err)
	}

	delayedCount, err := rc.ZCard(ctx, "delayed:test-queue").Result()
	if err != nil {
		t.Errorf("ScheduleDue Failed, %v", err)
	}
	if delayedCount != 1 {
		t.Errorf("Expected 1 Job in delayed queue, got %d", delayedCount)
	}

	activeCount, err := rc.ZCard(ctx, "queue:test-queue").Result()
	if err != nil {
		t.Errorf("Failed to check active queue: %v", err)
	}
	if activeCount != 0 {
		t.Errorf("Expected 0 jobs in active queue, got %d", activeCount)
	}
}

func TestCancelJob(t *testing.T) {
	rc := testutil.SetupRedis(t)
	rs := store.NewRedisStore(rc)
	ctx := context.Background()

	job := &models.Job{
		ID:       "job-1",
		Queue:    "test-queue",
		Type:     "test-task",
		Priority: 5,
	}

	if err := rs.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue Failed: %v", err)
	}

	if err := rs.CancelJob(ctx, job.ID); err != nil {
		t.Fatalf("CancelJob Failed: %v", err)
	}

	canceledJob, err := rs.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("Get Job Failed: %v", err)
	}
	if canceledJob == nil {
		t.Fatalf("Expected Job to exist, but got nil")
	}
	if canceledJob.Status != models.StatusCancelled {
		t.Errorf("Expected Status %s, got %s", models.StatusCancelled.String(), canceledJob.Status.String())
	}

	activeCount, err := rc.ZCard(ctx, "queue:test-queue").Result()
	if err != nil {
		t.Errorf("Failed to check active queue: %v", err)
	}
	if activeCount != 0 {
		t.Errorf("Expected Active job count 0, got %d", activeCount)
	}
}

func TestMoveToDeadLetter(t *testing.T) {
	rc := testutil.SetupRedis(t)
	rs := store.NewRedisStore(rc)
	ctx := context.Background()

	job := &models.Job{
		ID:       "job-1",
		Queue:    "test-queue",
		Type:     "test-task",
		Priority: 5,
	}

	if err := rs.MoveToDeadLetter(ctx, job); err != nil {
		t.Fatalf("Enqueue Failed: %v", err)
	}

	activeCount, err := rc.ZCard(ctx, "queue:test-queue").Result()
	if err != nil {
		t.Fatalf("Failed to check active queue: %v", err)
	}
	if activeCount != 0 {
		t.Errorf("Expected Active Job count %d, got %d", 0, activeCount)
	}

	deadCount, err := rc.LLen(ctx, "dead:test-queue").Result()
	if err != nil {
		t.Fatalf("Failed to check dead queue: %v", err)
	}
	if deadCount != 1 {
		t.Errorf("Expected Dead job count %d, got %d", 0, deadCount)
	}

	deadJob, err := rs.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("Failed to get job: %v", err)
	}
	if deadJob == nil {
		t.Fatalf("Expected Job to exist, but got nil")
	}
	if deadJob.Status != models.StatusDead {
		t.Errorf("Expected Job Status %s, got %s", deadJob.Status.String(), models.StatusDead.String())
	}
}

func TestAck(t *testing.T) {
	rc := testutil.SetupRedis(t)
	rs := store.NewRedisStore(rc)
	ctx := context.Background()

	job := &models.Job{
		ID:       "job-1",
		Queue:    "test-queue",
		Type:     "test-task",
		Priority: 5,
	}

	if err := rs.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue Failed: %v", err)
	}

	if err := rs.Ack(ctx, job.ID); err != nil {
		t.Fatalf("Ack Failed: %v", err)
	}

	ackedJob, err := rs.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob Failed: %v", err)
	}
	if ackedJob == nil {
		t.Fatalf("Expected job to exist, got nil")
	}

	if ackedJob.Status != models.StatusCompleted {
		t.Errorf("Expected status %s, got %s", models.StatusCompleted.String(), ackedJob.Status.String())
	}

	if ackedJob.CompletedAt == nil {
		t.Errorf("Expected CompleteAt to be set, but got nil")
	}
}
