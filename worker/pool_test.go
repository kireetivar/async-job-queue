package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kireetivar/async-job-queue/models"
)

func TestWorkerPool_StartStop(t *testing.T) {
	mockStore := &MockStore{
		DequeueFunc: func(ctx context.Context, queues []string) (*models.Job, error) {
			return nil, nil
		},
	}
	registry := NewHandlerRegistry()
	retryEngine := NewRetryEngine(mockStore, JitterRetryStrategy)

	wp := NewWorkerPool(2, []string{"test-queue"}, mockStore, registry, &retryEngine)
	wp.Start()

	done := make(chan struct{})
	go func() {
		wp.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return within 2 seconds")
	}
}

func TestWorkerPool_ProcessJob(t *testing.T) {
	var once sync.Once
	handlerCalled := make(chan struct{})

	mockStore := &MockStore{
		DequeueFunc: func(ctx context.Context, queues []string) (*models.Job, error) {
			var job *models.Job
			once.Do(func() {
				job = &models.Job{
					ID:      "job-1",
					Queue:   "test-queue",
					Type:    "test-job",
					Payload: json.RawMessage(`{"key":"value"}`),
				}
			})
			if job == nil {
				time.Sleep(50 * time.Millisecond)
			}
			return job, nil
		},
	}

	registry := NewHandlerRegistry()
	registry.Register("test-job", func(ctx context.Context, job *models.Job) error {
		close(handlerCalled)
		return nil
	})

	retryEngine := NewRetryEngine(mockStore, JitterRetryStrategy)
	wp := NewWorkerPool(1, []string{"test-queue"}, mockStore, registry, &retryEngine)
	wp.Start()

	select {
	case <-handlerCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not called within 2 seconds")
	}
	wp.Stop()

	if !mockStore.AckCalled {
		t.Errorf("expected Ack to be called")
	}
}

func TestWorkerPool_PausedQueue(t *testing.T) {
	var once sync.Once

	mockStore := &MockStore{
		DequeueFunc: func(ctx context.Context, queues []string) (*models.Job, error) {
			var job *models.Job
			once.Do(func() {
				job = &models.Job{
					ID:    "job-1",
					Queue: "test-queue",
					Type:  "test_job",
				}
			})
			if job == nil {
				time.Sleep(50 * time.Millisecond)
			}
			return job, nil
		},
		IsQueuePausedFunc: func(ctx context.Context, name string) (bool, error) {
			return true, nil
		},
	}

	registry := NewHandlerRegistry()
	handlerCalled := false
	registry.Register("test_job", func(ctx context.Context, job *models.Job) error {
		handlerCalled = true
		return nil
	})

	retryEngine := NewRetryEngine(mockStore, JitterRetryStrategy)
	wp := NewWorkerPool(1, []string{"test-queue"}, mockStore, registry, &retryEngine)
	wp.Start()

	time.Sleep(200 * time.Millisecond)
	wp.Stop()

	if !mockStore.EnqueueCalled {
		t.Error("expected job to be re-enqueued when is queue is paused")
	}
	if handlerCalled {
		t.Error("expected handler NOT to be called when queue is paused")
	}
}

func TestWorkerPool_HandlerError(t *testing.T) {
	var once sync.Once
	mockStore := &MockStore{
		DequeueFunc: func(ctx context.Context, queues []string) (*models.Job, error) {
			var job *models.Job
			once.Do(func() {
				job = &models.Job{
					ID:         "job-1",
					Queue:      "test-queue",
					Type:       "test_job",
					MaxRetries: 3,
				}
			})
			if job == nil {
				time.Sleep(50 * time.Millisecond)
			}
			return job, nil
		},
	}
	registry := NewHandlerRegistry()
	registry.Register("test_job", func(ctx context.Context, job *models.Job) error {
		return errors.New("handler failed")
	})
	retryEngine := NewRetryEngine(mockStore, JitterRetryStrategy)
	wp := NewWorkerPool(1, []string{"test-queue"}, mockStore, registry, &retryEngine)
	wp.Start()
	time.Sleep(200 * time.Millisecond)
	wp.Stop()
	if !mockStore.EnqueueCalled {
		t.Error("expected retry engine to re-enqueue the failed job")
	}
}
func TestWorkerPool_UnregisteredJobType(t *testing.T) {
	var once sync.Once
	mockStore := &MockStore{
		DequeueFunc: func(ctx context.Context, queues []string) (*models.Job, error) {
			var job *models.Job
			once.Do(func() {
				job = &models.Job{
					ID:         "job-1",
					Queue:      "test-queue",
					Type:       "unknown_type",
					MaxRetries: 3,
				}
			})
			if job == nil {
				time.Sleep(50 * time.Millisecond)
			}
			return job, nil
		},
	}
	registry := NewHandlerRegistry()
	retryEngine := NewRetryEngine(mockStore, JitterRetryStrategy)
	wp := NewWorkerPool(1, []string{"test-queue"}, mockStore, registry, &retryEngine)
	wp.Start()
	time.Sleep(200 * time.Millisecond)
	wp.Stop()
	if !mockStore.EnqueueCalled {
		t.Error("expected retry engine to handle unregistered job type")
	}
}
