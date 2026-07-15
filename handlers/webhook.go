package handlers

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/kireetivar/async-job-queue/metrics"
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

type circuitState struct {
	failures     int
	blockedUntil time.Time
}

var breakers sync.Map

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

		host, err := url.Parse(wh.URL)
		if err != nil {
			return fmt.Errorf("invalid webhook URL: %w", err)
		}
		if host.Host == "" {
			return fmt.Errorf("invalid webhook URL: empty host")
		}
		if c, ok := breakers.Load(host.Host); ok {
			state := c.(circuitState)
			if time.Now().Before(state.blockedUntil) {
				return fmt.Errorf("circuit open for host: %s, blockedUntil: %v", host.Host, state.blockedUntil)
			}
		}

		req, err := http.NewRequestWithContext(ctx, wh.Method, wh.URL, bytes.NewReader(wh.Body))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		for k, v := range wh.Headers {
			req.Header.Set(k, v)
		}
		if secret != "" {
			hasher := hmac.New(sha256.New, []byte(secret))
			hasher.Write(wh.Body)
			signature := hex.EncodeToString(hasher.Sum(nil))
			req.Header.Set("X-Webhook-Signature", signature)
		}

		client := http.Client{Timeout: time.Duration(wh.TimeoutSeconds) * time.Second}
		start := time.Now()
		resp, err := client.Do(req)
		metrics.WebhookDuration.Observe(time.Since(start).Seconds())
		metrics.WebhookRequests.Inc()
		if err != nil {
			recordFailure(host.Host)
			metrics.WebhookFailures.Inc()
			slog.Warn("webhook request failed", "job_id", job.ID, "url", wh.URL, "error", err.Error())
			return fmt.Errorf("failed to send webhook request: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			recordFailure(host.Host)
			metrics.WebhookFailures.Inc()
			slog.Warn("webhook request failed", "job_id", job.ID, "url", wh.URL, "status", resp.Status)
			return fmt.Errorf("webhook returned non-success status: %s", resp.Status)
		}
		breakers.Delete(host.Host)
		slog.Info("webhook delivered", "job_id", job.ID, "url", wh.URL, "status", resp.StatusCode)
		return nil
	}
}

func recordFailure(host string) {
	if c, ok := breakers.Load(host); ok {
		state := c.(circuitState)
		newFailures := state.failures + 1
		newBlockedUntil := state.blockedUntil
		if newFailures > 5 {
			newBlockedUntil = time.Now().Add(time.Minute)
		}
		breakers.Store(host, circuitState{
			failures:     newFailures,
			blockedUntil: newBlockedUntil,
		})
	} else {
		breakers.Store(host, circuitState{
			failures: 1,
		})
	}
}
