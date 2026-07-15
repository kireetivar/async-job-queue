package handlers

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kireetivar/async-job-queue/models"
)

func TestNewWebhookHandler_Success(t *testing.T) {
	var gotMethod, gotSig string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotSig = r.Header.Get("X-Webhook-Signature")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	body := json.RawMessage(`{"hello":"world"}`)
	payload, err := json.Marshal(map[string]any{
		"url":    srv.URL,
		"method": "POST",
		"body":   body,
	})
	if err != nil {
		t.Fatal(err)
	}

	secret := "test-secret"
	handler := NewWebhookHandler(secret)
	job := &models.Job{
		ID:      "job-1",
		Payload: payload,
		Type:    "webhook",
	}

	if err := handler(context.Background(), job); err != nil {
		t.Fatalf("expected nil, got %v\n", err)
	}

	if gotMethod != "POST" {
		t.Errorf(" expected method %q, got POST", gotMethod)
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	wantSig := hex.EncodeToString(mac.Sum(nil))
	if gotSig != wantSig {
		t.Errorf("expected signature %q, got %q", wantSig, gotSig)
	}

	if !bytes.Equal(gotBody, body) {
		t.Errorf("expected body %q, got %q", body, gotBody)
	}
}
