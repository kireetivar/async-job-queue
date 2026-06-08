package worker

import (
	"context"
	"testing"
	"time"

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
		mockBackoff := func(attempt int) time.Duration {
			return 5 * time.Minute
		}

		t.Run(tt.name, func(t *testing.T) {
			mockStore := &MockStore{
				EnqueueError:          tt.enqueuedErr,
				MoveToDeadLetterError: tt.deadLetterErr,
			}
			engine := NewRetryEngine(mockStore, mockBackoff)

			err := engine.Handle(context.Background(), tt.job, tt.jobErr)
			if (err != nil)  != tt.wantErr {
				t.Errorf("Handle() error= %v, wantErr %v", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if mockStore.EnqueueCalled != tt.expectedEnque {
				t.Errorf("expected EnqueueCalled=%v, got %v", tt.expectedEnque, mockStore.EnqueueCalled)
			}

			if tt.expectedEnque {
				if tt.job.Status != models.StatusEnqueued {
					t.Errorf("expected status %s, got %s", models.StatusEnqueued, tt.job.Status)
				}
				if tt.job.RetryAt == nil {
					t.Errorf("expected retry at to be non-nil")
				}
			}

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
