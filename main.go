package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kireetivar/async-job-queue/api"
	"github.com/kireetivar/async-job-queue/models"
	"github.com/kireetivar/async-job-queue/scheduler"
	"github.com/kireetivar/async-job-queue/store"
	"github.com/kireetivar/async-job-queue/worker"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})

	rs := store.NewRedisStore(rdb)

	router := api.NewRouter(rs)

	retryEngine := worker.NewRetryEngine(rs, worker.JitterRetryStrategy)

	handlerRegistry := worker.NewHandlerRegistry()
	handlerRegistry.Register("test_job", func(ctx context.Context, job *models.Job) error {
		log.Printf("✅ Processing job %s | type: %s | payload: %s", job.ID, job.Type, string(job.Payload))
		return nil
	})

	queues := []string{"email", "mobile"}
	wp := worker.NewWorkerPool(10, queues, rs, handlerRegistry, &retryEngine)

	wp.Start()

	sc := scheduler.NewScheduler(rs, 5*time.Second)

	go sc.Run(context.Background())

	go func() {
		log.Println(":: Server starting on :8080")
		if err := router.Run(":8080"); err != nil {
			log.Fatal(err)
		}
	}()

	// wait for (ctrl + c)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println(":: Shutting down server...")

	wp.Stop()
	sc.Stop()
	rdb.Close()

	log.Print("ShutDown Successful")
}
