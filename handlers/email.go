package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/kireetivar/async-job-queue/models"
)

type email struct {
	To      string `json:"to"`
	From    string `json:"from"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func EmailHandler(ctx context.Context, job *models.Job) error {
	var e email
	err := json.Unmarshal(job.Payload, &e)
	if err != nil {
		return fmt.Errorf("failed to unmarshal email payload: %w", err)
	}
	if e.To == "" {
		return fmt.Errorf("invalid To address: %s", e.To)
	}
	if e.From == "" {
		return fmt.Errorf("invalid From address: %s", e.From)
	}
	if e.Subject == "" {
		return fmt.Errorf("invalid Subject: %s", e.Subject)
	}
	if e.Body == "" {
		return fmt.Errorf("invalid Body: %s", e.Body)
	}
	time.Sleep(time.Duration(rand.IntN(1500)) * time.Millisecond)
	slog.Info("email sent", slog.String("to", e.To), slog.String("from", e.From), slog.String("subject", e.Subject))
	return nil
}
