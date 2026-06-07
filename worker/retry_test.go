package worker

import (
	"testing"

	"github.com/kireetivar/async-job-queue/models"
)

func TestRetryEngine_Handle(t *testing.T) {
	tests := []struct {
		name          string
		job           *models.Job
		jobErr        string
		enqueuedErr   error
		deadLetterErr error
		wantErr       bool
		expectedEnque bool
		expectedDLQ   bool

		expectedStatus models.JobStatus
	}{
		{
			"Job retry within limit",
			&models.Job{ID: "job-1", RetryCount: 0, MaxRetries: 3},
			"temporary failure",
			nil, nil,
			false, true, false,
			models.StatusEnqueued,
		},
		{
			"Job retry exceeding max retries",
			&models.Job{ID: "job-2", RetryCount: 2, MaxRetries: 3},
			"fatal failure",
			nil, nil,
			false, false, true,
			models.StatusDead,
		},
		{
			"Nil Job",
			nil,
			"some error",
			nil, nil,
			true, false, false,
			"",
		},
		{
			"Empty job error",
			&models.Job{ID: "job-3", RetryCount: 0, MaxRetries: 3},
			"",
			nil, nil,
			true, false, false,
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			
		})
	}
}

func TestJitterRetryStrategy(t *testing.T) {
	for attempt := 1; attempt < 10; attempt++ {
		duration := JitterRetryStrategy(attempt)
		if duration <= 0 {
			t.Errorf("attempt: %d: expected positive duration, got %v", attempt, duration)
		}
	}
}
