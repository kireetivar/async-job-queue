package models

import (
	"encoding/json"
	"fmt"
	"time"
)

// JobStatus represents the status of a job.
// @Description Status of a job (enqueued, running, completed, failed, dead, cancelled)
type JobStatus string

type Job struct {
	ID          string          `json:"id"`
	Queue       string          `json:"queue"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      JobStatus       `json:"status" enums:"enqueued,running,completed,failed,dead,cancelled" example:"enqueued"`
	Priority    int             `json:"priority"`
	MaxRetries  int             `json:"max_retries"`
	RetryCount  int             `json:"retry_count"`
	CreatedAt   time.Time       `json:"created_at"`
	RunAt       *time.Time      `json:"run_at,omitempty"`
	RetryAt     *time.Time      `json:"retry_at,omitempty"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	Error       string          `json:"error,omitempty"`
	WorkerID    string          `json:"worker_id,omitempty"`
}

const (
	StatusEnqueued  JobStatus = "enqueued"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
	StatusDead      JobStatus = "dead"
	StatusCancelled JobStatus = "cancelled"
)

func (s JobStatus) String() string {
	if s == "" {
		return string(StatusEnqueued)
	}
	return string(s)
}

func (s *JobStatus) UnmarshalJSON(b []byte) error {
	var str string
	if err := json.Unmarshal(b, &str); err == nil {
		if str == "" {
			*s = StatusEnqueued
			return nil
		}
		*s = JobStatus(str)
		return nil
	}

	var val int
	if err := json.Unmarshal(b, &val); err == nil {
		switch val {
		case 0:
			*s = StatusEnqueued
		case 1:
			*s = StatusRunning
		case 2:
			*s = StatusCompleted
		case 3:
			*s = StatusFailed
		case 4:
			*s = StatusDead
		case 5:
			*s = StatusCancelled
		default:
			*s = StatusEnqueued
		}
		return nil
	}

	return fmt.Errorf("invalid job status: %s", string(b))
}


