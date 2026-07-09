package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/kireetivar/async-job-queue/models"
	"github.com/kireetivar/async-job-queue/worker"
)

type webhook struct {
	URL            string            `json:"url"`
	Method         string            `json:"method"`
	Headers        map[string]string `json:"headers"`
	Body           json.RawMessage   `json:"body"`
	TimeoutSeconds int               `json:"timeout_seconds"`
}

func NewWebhookHandler(secret string) worker.HandleFunc {
	return func(ctx context.Context, job *models.Job) error {
		var wh webhook
		err := json.Unmarshal(job.Payload, &wh)
		if err != nil {
			return fmt.Errorf("failed to unmarshal webhook payload: %w", err)
		}
		if wh.URL == "" {
			return fmt.Errorf("invalid URL: %s", wh.URL)
		}
		if wh.Method == "" {
			wh.Method = "POST"
		}
		if wh.TimeoutSeconds <= 0 {
			wh.TimeoutSeconds = 10
		}

		req, err := http.NewRequestWithContext(ctx, wh.Method, wh.URL, bytes.NewReader(wh.Body))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		for k, v := range wh.Headers {
			req.Header.Set(k, v)
		}
		client := http.Client{Timeout: time.Duration(wh.TimeoutSeconds) * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to send webhook request: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("webhook returned non-success status: %s", resp.Status)
		}
		slog.Info("webhook delivered", "job_id", job.ID, "url", wh.URL, "status", resp.StatusCode)
		return nil
	}
}
