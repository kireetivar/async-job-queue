package worker

import (
	"context"
	"sync"

	"github.com/kireetivar/async-job-queue/models"
	"github.com/kireetivar/async-job-queue/store"
)

type WorkerPool struct {
	concurrency int
	queues      []string
	store       store.Store
	registry    *HandleRegistry
	wg          sync.WaitGroup
	stopCh      chan struct{}
}

func NewWorkerPool(concurrency int, queues []string, store store.Store, r *HandleRegistry) *WorkerPool {
	return &WorkerPool{
		concurrency: concurrency,
		queues:      queues,
		store:       store,
		registry:    r,
		wg:          sync.WaitGroup{},
		stopCh:      make(chan struct{}),
	}
}

func (wp *WorkerPool) Start() {
	for i := 0; i < wp.concurrency; i++ {
		wp.wg.Add(1)
		go func() {
			defer wp.wg.Done()
			wp.work()
		}()
	}
}

func (wp *WorkerPool) work() {
	for {
		select {
		case <-wp.stopCh:
			// someone closed the stopCh
			return
		default:
			ctx := context.Background()
			job, err := wp.store.Dequeue(ctx, wp.queues)
			if err != nil {
				continue // store(redis) error, try again
			}
			if job == nil {
				continue // BZPopMin timed out, no job avaliable, try again
			}
			fn, err := wp.registry.Get(job.Type)
			if err != nil {
				continue // job is not registered, incorrect job name
			}
			err = fn(ctx, job)
			if err != nil {
				job.Status = models.StatusFailed
				job.Error = err.Error()
				wp.store.Enqueue(ctx, job) // TODO: Should write retry logic here
				continue
			}
			wp.store.Ack(ctx, job.ID)
		}
	}
}

func (wp *WorkerPool) Stop() {
	close(wp.stopCh)
	wp.wg.Wait()
}
