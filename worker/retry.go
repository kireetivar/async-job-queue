package worker

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/kireetivar/async-job-queue/models"
	"github.com/kireetivar/async-job-queue/store"
)

const base float64 = 1

type BackoffFunc func(attempt int) time.Duration

type RetryEngine struct {
	store     store.Store
	backoffFn BackoffFunc
}

func (re *RetryEngine) Handle(ctx context.Context, job *models.Job, jobErr string) error {
	if job == nil {
		return fmt.Errorf("Job must not be nil while retry")
	}
	if jobErr == "" {
		return fmt.Errorf("Joberr can't be empty while retry")
	}
	job.RetryCount++
	job.Error = jobErr

	if job.RetryCount >= job.MaxRetries {
		return re.store.MoveToDeadLetter(ctx, job)
	}

	delay := re.backoffFn(job.RetryCount)
	retryAt := time.Now().Add(delay)
	job.RetryAt = &retryAt
	job.Status = models.StatusEnqueued

	return re.store.Enqueue(ctx, job)
}

func JitterRetryStrategy(attempt int) time.Duration {
	exp := math.Min(base*math.Pow(2, float64(attempt-1)), 10)
	jitter := 0.8 + rand.Float64()*(1.2-0.8)
	return time.Duration(exp * jitter * float64(time.Second))
}

func NewRetryEngine(store store.Store, backoffFn BackoffFunc) RetryEngine {
	return RetryEngine{
		store:     store,
		backoffFn: backoffFn,
	}
}
