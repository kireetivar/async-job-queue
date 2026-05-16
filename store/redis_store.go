package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
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
	if job.RunAt != nil && job.RunAt.After(time.Now()) {
		// Delayed job — add to delayed set
		pipe.ZAdd(ctx, "delayed:"+job.Queue, redis.Z{
			Score:  float64(job.RunAt.Unix()),
			Member: job.ID,
		})
	} else {
		pipe.ZAdd(ctx, "queue:"+job.Queue, redis.Z{
			Score:  float64(job.Priority),
			Member: job.ID,
		})
	}

	_, err := pipe.Exec(ctx)
	return err
}

func (s *RedisStore) Dequeue(ctx context.Context, queues []string) (*models.Job, error) {
	keys := make([]string, len(queues))
	for i, v := range queues {
		keys[i] = "queue:" + v
	}
	val, err := s.client.BZPopMin(ctx, 5*time.Second, keys...).Result()
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

	job := parseJobFromMap(jobMap)
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

func (s *RedisStore) ScheduleDue(ctx context.Context) ([]*models.Job, error) {
	var jobIds []string
	vals, err := s.client.Keys(ctx, "delayed:*").Result()
	if err != nil {
		return nil, err
	}
	for _, v := range vals {
		j, err := s.client.ZRangeArgs(ctx, redis.ZRangeArgs{
			Key:     v,
			Start:   0,
			Stop:    strconv.FormatInt(time.Now().Unix(), 10),
			ByScore: true,
		}).Result()
		if err != nil {
			return nil, err
		}
		if len(j) > 0 {
			members := make([]any, len(j))
			for i, id := range j {
				members[i] = id
			}
			s.client.ZRem(ctx, v, members...)
		}
		jobIds = append(jobIds, j...)
	}

	var jobs []*models.Job
	for _, v := range jobIds {
		jobMap, err := s.client.HGetAll(ctx, "job:"+v).Result()
		if err != nil {
			return nil, err
		}
		if len(jobMap) == 0 {
			continue
		}

		job := parseJobFromMap(jobMap)
		jobs = append(jobs, job)
	}

	return jobs, nil
}

func (s *RedisStore) GetJob(ctx context.Context, jobId string) (*models.Job, error) {
	jobMap, err := s.client.HGetAll(ctx, "job:"+jobId).Result()
	if err != nil {
		return nil, err
	}
	if len(jobMap) == 0 {
		return nil, nil
	}
	return parseJobFromMap(jobMap), nil
}

func (s *RedisStore) CancelJob(ctx context.Context, jobId string) error {
	jobMap, err := s.client.HGetAll(ctx, "job:"+jobId).Result()
	if err != nil {
		return err
	}
	if len(jobMap) == 0 {
		return fmt.Errorf("Job %s not found", jobId)
	}

	pipe := s.client.TxPipeline()
	pipe.ZRem(ctx, "queue:"+jobMap["queue"], jobId)
	pipe.HSet(ctx, "job:"+jobId, "status", int(models.StatusFailed))
	_, err = pipe.Exec(ctx)

	return err
}

func (s *RedisStore) ListQueues(ctx context.Context) ([]models.QueueInfo, error) {
	var queueInfoList []models.QueueInfo
	queues, err := s.client.Keys(ctx, "queue:*").Result()
	if err != nil {
		return nil, err
	}
	for _, v := range queues {
		count, err := s.client.ZCard(ctx, v).Result()
		if err != nil {
			return queueInfoList, err
		}

		queueName := strings.TrimPrefix(v, "queue:")

		paused, err := s.client.Exists(ctx, "paused:"+queueName).Result()
		if err != nil {
			return queueInfoList, err
		}
		queueInfoList = append(queueInfoList, models.QueueInfo{
			Name:   queueName,
			Depth:  count,
			Paused: paused > 0,
		})
	}

	return queueInfoList, nil
}

func (s *RedisStore) PauseQueue(ctx context.Context, name string) error {
	return s.client.Set(ctx, "paused:"+name, "1", 0).Err()
}

func (s *RedisStore) ResumeQueue(ctx context.Context, name string) error {
	return s.client.Del(ctx, "paused:"+name).Err()
}

func (s *RedisStore) GetQueueStatus(ctx context.Context) ([]models.QueueStats, error) {
	nameSet := make(map[string]bool)

	for _, pattern := range []struct{ prefix, glob string }{
		{"queue:", "queue:*"},
		{"dead:", "dead:*"},
		{"delayed:", "delayed:*"},
	} {
		keys, err := s.client.Keys(ctx, pattern.glob).Result()
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			nameSet[strings.TrimPrefix(k, pattern.prefix)] = true
		}
	}

	queueStatusList := make([]models.QueueStats, 0, len(nameSet))
	for name := range nameSet {
		pending, _ := s.client.ZCard(ctx, "queue:"+name).Result()
		dead, _ := s.client.LLen(ctx, "dead:"+name).Result()
		paused, _ := s.client.Exists(ctx, "paused:"+name).Result()

		queueStatusList = append(queueStatusList, models.QueueStats{
			Name:    name,
			Pending: pending,
			Dead:    dead,
			Paused:  paused > 0,
		})
	}
	return queueStatusList, nil
}

func parseJobFromMap(jobMap map[string]string) *models.Job {
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
	return job
}
