package worker

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kireetivar/async-job-queue/models"
	"github.com/kireetivar/async-job-queue/store"
	"github.com/kireetivar/async-job-queue/testutil"
)

func TestE2E_EnqqueueProcessSuccess(t *testing.T) {
	rc := testutil.SetupRedis(t)
	rs := store.NewRedisStore(rc)
	ctx := context.Background()

	var handled atomic.Bool
	reg := NewHandlerRegistry()
	if err := reg.Register("e2e_ok", func(ctx context.Context, job *models.Job) error {
		handled.Store(true)
		return nil
	}); err != nil {
		t.Fatalf("error: %v", err)
	}

	retryEngine := NewRetryEngine(rs, JitterRetryStrategy)
	wp := NewWorkerPool(2, []string{"e2e"}, rs, reg, &retryEngine)
	wp.Start()
	t.Cleanup(func() {wp.Stop()})


	job := &models.Job {
		ID: "job-id",
		Queue: "e2e",
		Type: "e2e_ok",
		Payload: json.RawMessage(`{"hello": "world"}`),
	}

	if err := rs.Enqueue(ctx, job);  err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := rs.GetJob(ctx, job.ID)
		if err != nil {
			t.Fatalf("get job: %v", err)
		}
		if got != nil && got.Status == models.StatusCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waititng for completed; stattus: %v handled: %v", statusOrNil(got), handled.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !handled.Load() {
		t.Fatal("handler was never called")
	}

}

func statusOrNil(j *models.Job) any {
    if j == nil {
        return nil
    }
    return j.Status
}