package handlers

import "github.com/kireetivar/async-job-queue/worker"

type Config struct {
	WebHookSecret string
}

func RegisterAll(registry *worker.HandleRegistry, cfg Config) error {
	if err := registry.Register("email", EmailHandler); err != nil {
		return err
	}
	if err := registry.Register("webhook", NewWebhookHandler(cfg.WebHookSecret)); err != nil {
		return err
	}
	return nil
}
