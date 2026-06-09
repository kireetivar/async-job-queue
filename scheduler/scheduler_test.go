package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/kireetivar/async-job-queue/models"
	"github.com/kireetivar/async-job-queue/store"
)

// Ensure SchedulerMockStore implements store.Store
type SchedulerMockStore struct {
	store.Store
	ScheduleDueCallCount int
	ScheduleDueErr       error
}

func (m *SchedulerMockStore) ScheduleDue(ctx context.Context) ([]*models.Job, error) {
	m.ScheduleDueCallCount++
	return nil, m.ScheduleDueErr
}

func TestScheduler_Run_CallsScheduleDue(t *testing.T) {
	mockstore := &SchedulerMockStore{}
	scheduler := NewScheduler(mockstore, 50*time.Millisecond)

	go func() {
		scheduler.Run(context.Background())
	}()

	time.Sleep(150 * time.Millisecond)
	scheduler.Stop()

	if mockstore.ScheduleDueCallCount < 2 {
		t.Errorf("expected ScheduleDueCallCount be more than 2, got %v", mockstore.ScheduleDueCallCount)
	}
}

func TestScheduler_Stop(t *testing.T) {
	mockstore := &SchedulerMockStore{}
	scheduler := NewScheduler(mockstore, 50*time.Millisecond)
	c := make(chan struct{})

	go func() {
		scheduler.Run(context.Background())
		c <- struct{}{}
	}()

	scheduler.Stop()

	select {
	case <-c:
	case <-time.After(1 * time.Second):
		t.Errorf("expected duration be less than 1 second, got more than 1 second")
	}
}

func TestScheduler_ContextCancel(t *testing.T) {
	mockstore := &SchedulerMockStore{}
	scheduler := NewScheduler(mockstore, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan struct{})

	go func() {
		scheduler.Run(ctx)
		c <- struct{}{}
	}()

	cancel()

	select {
	case <-c:
	case <-time.After(1 * time.Second):
		t.Errorf("expected Run to exit after context cancel, but it didn't within 1 second")
	}
}
