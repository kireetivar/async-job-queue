package worker

import (
	"context"
	"sync"
	"time"

	"github.com/kireetivar/async-job-queue/metrics"
	"github.com/kireetivar/async-job-queue/store"
)

type WorkerPool struct {
	concurrency int
	queues      []string
	store       store.Store
	registry    *HandleRegistry
	retryEngine *RetryEngine
	wg          sync.WaitGroup
	stopCh      chan struct{}
}

func NewWorkerPool(concurrency int, queues []string, store store.Store, r *HandleRegistry, retryEngine *RetryEngine) *WorkerPool {
	return &WorkerPool{
		concurrency: concurrency,
		queues:      queues,
		store:       store,
		registry:    r,
		retryEngine: retryEngine,
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
			isPaused, err := wp.store.IsQueuePaused(ctx, job.Queue)
			if err != nil {
				continue
			}
			if isPaused {
				err = wp.store.Enqueue(ctx, job)
				if err != nil {
					continue
				}
				time.Sleep(1 * time.Second)
				continue
			}
			metrics.ActiveWorkers.Inc()
			fn, err := wp.registry.Get(job.Type)
			if err != nil {
				wp.retryEngine.Handle(ctx, job, err.Error())
				metrics.ActiveWorkers.Dec()
				continue
			}
			start := time.Now()
			err = fn(ctx, job)
			metrics.JobProcessingDuration.Observe(time.Since(start).Seconds())
			if err != nil {
				wp.retryEngine.Handle(ctx, job, err.Error()) // handler failed, retry/dead letter
				metrics.JobsFailed.Inc()
				metrics.ActiveWorkers.Dec()
				continue
			}
			wp.store.Ack(ctx, job.ID)
			metrics.JobsProcessed.Inc()
			metrics.ActiveWorkers.Dec()
		}
	}
}

func (wp *WorkerPool) Stop() {
	close(wp.stopCh)
	wp.wg.Wait()
}
