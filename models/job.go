package models

import (
	"encoding/json"
	"time"
)

type JobStatus int

type Job struct {
	ID          string          `json:"id"`
	Queue       string          `json:"queue"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      JobStatus       `json:"status"`
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
	StatusEnqueued JobStatus = iota
	StatusRunning
	StatusCompleted
	StatusFailed
	StatusDead
	StatusCancelled
)

func (s JobStatus) String() string {
	switch s {
	case StatusEnqueued:
		return "enqueued"
	case StatusRunning:
		return "running"
	case StatusCompleted:
		return "completed"
	case StatusFailed:
		return "failed"
	case StatusDead:
		return "dead"
	default:
		return "unknown"
	}
}
