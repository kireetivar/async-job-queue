package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kireetivar/async-job-queue/api"
	"github.com/kireetivar/async-job-queue/config"
	_ "github.com/kireetivar/async-job-queue/docs"
	"github.com/kireetivar/async-job-queue/models"
	"github.com/kireetivar/async-job-queue/scheduler"
	"github.com/kireetivar/async-job-queue/store"
	"github.com/kireetivar/async-job-queue/worker"
	"github.com/redis/go-redis/v9"
)

// @title           Async Job Queue API
// @version         1.0
// @description     A distributed async job queue system built with Go and Redis.
// @contact.name    Kireeti Varma
// @contact.url     https://github.com/kireetivar/async-job-queue
// @license.name    MIT
// @license.url     https://opensource.org/licenses/MIT
// @host            localhost:9090
// @BasePath        /api/v1
// @tag.name        jobs
// @tag.description All operations related to creating, retrieving, cancelling, and retrying jobs
// @tag.name        queues
// @tag.description Operations for managing queues (list, pause, resume)
// @tag.name        monitoring
// @tag.description Health checks and dashboard statistics
func main() {
	cfg := config.Load()

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	rs := store.NewRedisStore(rdb)

	router := api.NewRouter(rs)

	retryEngine := worker.NewRetryEngine(rs, worker.JitterRetryStrategy)

	handlerRegistry := worker.NewHandlerRegistry()
	err := handlerRegistry.Register("test_job", func(ctx context.Context, job *models.Job) error {
		time.Sleep(10 * time.Second)
		slog.Info("processing job", "job_id", job.ID, "type", job.Type, "payload", string(job.Payload))
		return nil
	})
	if err != nil {
		fmt.Printf("Unable to register handler `test_job`: %v", err)
	}

	wp := worker.NewWorkerPool(cfg.WorkerCount, cfg.Queues, rs, handlerRegistry, &retryEngine)

	wp.Start()

	sc := scheduler.NewScheduler(rs, cfg.ScheduleInterval)

	go sc.Run(context.Background())

	go func() {
		slog.Info("server starting", "port", cfg.ServerPort)
		if err := router.Run(cfg.ServerPort); err != nil {
			slog.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	// wait for (ctrl + c)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down server")

	wp.Stop()
	sc.Stop()
	rdb.Close()

	slog.Info("ShutDown Successful")
}
