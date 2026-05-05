package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/kireetivar/async-job-queue/models"
	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(client *redis.Client) *RedisStore {
	return &RedisStore{client: client}
}

func (s *RedisStore) Enqueue(ctx context.Context, job *models.Job) error {
	pipe := s.client.TxPipeline()

	jobData := map[string]any{
		"id":          job.ID,
		"queue":       job.Queue,
		"type":        job.Type,
		"payload":     string(job.Payload),
		"status":      int(job.Status),
		"priority":    job.Priority,
		"max_retries": job.MaxRetries,
		"retry_count": job.RetryCount,
		"created_at":  job.CreatedAt.Format(time.RFC3339), //preffered format
	}

	if job.RunAt != nil {
		jobData["run_at"] = job.RunAt.Format(time.RFC3339)
	}

	// Hashes are good for storing large objects
	pipe.HSet(ctx, "job:"+job.ID, jobData)
	// extremely fast at sorting
	pipe.ZAdd(ctx, "queue:"+job.Queue, redis.Z{
		Score:  float64(job.Priority),
		Member: job.ID,
	})

	_, err := pipe.Exec(ctx)

	return err
}

func (s *RedisStore) Dequeue(ctx context.Context, queues []string) (*models.Job, error) {
	keys := make([]string, len(queues))
	for i, v := range queues {
		keys[i] = "queue:" + v
	}
	val, err := s.client.BZPopMin(ctx, 5*time.Second, queues...).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // no job found after 5 sec
		}
		return nil, err
	}

	jobId := val.Member.(string)
	jobMap, err := s.client.HGetAll(ctx, "job:"+jobId).Result()
	if err != nil {
		return nil, err
	}
	if len(jobMap) == 0 {
		return nil, nil // jobdata was somehow deleted
	}

	priority, _ := strconv.Atoi(jobMap["priority"])
	status, _ := strconv.Atoi(jobMap["status"])
	maxRetries, _ := strconv.Atoi(jobMap["max_retries"])
	retryCount, _ := strconv.Atoi(jobMap["retry_count"])
	createdAt, _ := time.Parse(time.RFC3339, jobMap["created_at"])

	job := &models.Job{
		ID:         jobMap["id"],
		Queue:      jobMap["queue"],
		Type:       jobMap["type"],
		Payload:    []byte(jobMap["payload"]),
		Status:     models.JobStatus(status),
		Priority:   priority,
		MaxRetries: maxRetries,
		RetryCount: retryCount,
		CreatedAt:  createdAt,
	}

	if runAtStr := jobMap["run_at"]; runAtStr != "" {
		if t, err := time.Parse(time.RFC3339, runAtStr); err == nil {
			job.RunAt = &t
		}
	}

	return job, nil
}

func (s *RedisStore) Ack(ctx context.Context, jobId string) error {
	if jobId == "" {
		return fmt.Errorf("jobId must not be empty")
	}
	return s.client.HSet(ctx, "job:"+jobId,
		"status", int(models.StatusCompleted),
		"completed_at", time.Now().Format(time.RFC3339),
	).Err()
}

func (s *RedisStore) Nack(ctx context.Context, job *models.Job) error {
	if job == nil {
		return fmt.Errorf("job must not be nil")
	}

	updateJobMap := map[string]any{
		"status":      int(models.StatusFailed),
		"error":       job.Error,
		"retry_count": job.RetryCount,
		"max_retries": job.MaxRetries,
	}

	if job.RetryAt != nil {
		updateJobMap["retry_at"] = job.RetryAt.Format(time.RFC3339)
	}

	return s.client.HSet(ctx, "job:"+job.ID, updateJobMap).Err()
}

func (s *RedisStore) MoveToDeadLetter(ctx context.Context, job *models.Job) error {
	if job == nil {
		return fmt.Errorf("job must not be nil")
	}

	pipe := s.client.TxPipeline()

	job.Status = models.StatusDead
	jobData := map[string]any{
		"status":      int(job.Status),
		"max_retries": job.MaxRetries,
		"retry_count": job.RetryCount,
		"error":       job.Error,
	}
	if job.RetryAt != nil {
		jobData["retry_at"] = job.RetryAt.Format(time.RFC3339)
	}

	pipe.HSet(ctx, "job:"+job.ID, jobData)
	pipe.LPush(ctx, "dead:"+job.Queue, job.ID)

	_, err := pipe.Exec(ctx)

	return err
}
