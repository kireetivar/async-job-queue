package scheduler

import (
	"context"
	"time"

	"github.com/kireetivar/async-job-queue/store"
)

type Scheduler struct {
	store    store.Store
	interval time.Duration
	stopCh   chan struct{}
}

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			val, err := s.store.ScheduleDue(ctx)
			if err != nil {
				continue
			}
			for _, v := range val {
				err := s.store.Enqueue(ctx, v)
				if err != nil {
					continue
				}
			}
		case <-s.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *Scheduler) Stop() {
	close(s.stopCh)
}

func NewScheduler(store store.Store, interval time.Duration) *Scheduler {
	return &Scheduler{
		store:    store,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}
