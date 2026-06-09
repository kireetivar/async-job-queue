package worker

import (
	"context"
	"testing"

	"github.com/kireetivar/async-job-queue/models"
)

func TestRegister(t *testing.T) {
	testHandleFunc := func(ctx context.Context, jobs *models.Job) error { return nil }
	tests := []struct {
		name    string
		jobType string
		fn      HandleFunc
		wantErr bool
	}{
		{"a valid handler", "test", testHandleFunc, false},
		{"a nil handler", "test", nil, true},
		{"empty jobType", "", testHandleFunc, true},
		{"a duplicate handler", "test", testHandleFunc, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := NewHandlerRegistry()

			if tt.name == "a duplicate handler" {
				err := reg.Register(tt.jobType, tt.fn)
				if err != nil {
					t.Fatalf("Failed to setup duplicate test case: %v", err)
				}
			}
			err := reg.Register(tt.jobType, tt.fn)
			if (err != nil) != tt.wantErr {
				t.Errorf("Expected error: %v, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestGet(t *testing.T) {
	testHandleFunc := func(ctx context.Context, jobs *models.Job) error { return nil }
	tests := []struct {
		name         string
		jobType      string
		registeredFn HandleFunc
		wantErr      bool
	}{
		{"a registered handler", "test", testHandleFunc, false},
		{"a unregistered handler", "test", nil, true},
		{"empty jobType", "", testHandleFunc, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rg := NewHandlerRegistry()

			if tt.name == "a registered handler" {
				err := rg.Register(tt.jobType, tt.registeredFn)
				if err != nil {
					t.Fatalf("Failed to test case: %v", err)
				}
			}

			fn, err := rg.Get(tt.jobType)
			if tt.wantErr != (err != nil) {
				t.Errorf("Expected error: %v, got error: %v", tt.wantErr, err)
			}
			if !tt.wantErr && fn == nil {
				t.Errorf("Expected handler function to be returned, got nil")
			}
		})
	}
}
