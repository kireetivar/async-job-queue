package models

import (
	"encoding/json"
	"testing"
)

func TestJobStatus_MarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		status  JobStatus
		want    string
		wantErr bool
	}{
		{"StatusEnqueued", StatusEnqueued, `"enqueued"`, false},
		{"StatusRunning", StatusRunning, `"running"`, false},
		{"StatusCompleted", StatusCompleted, `"completed"`, false},
		{"StatusFailed", StatusFailed, `"failed"`, false},
		{"StatusDead", StatusDead, `"dead"`, false},
		{"StatusCancelled", StatusCancelled, `"cancelled"`, false},
		{"Arbitrary", JobStatus("custom_state"), `"custom_state"`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.status)
			if (err != nil) != tt.wantErr {
				t.Errorf("JobStatus.MarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if string(got) != tt.want {
				t.Errorf("JobStatus.MarshalJSON() = %s, want %s", string(got), tt.want)
			}
		})
	}
}

func TestJobStatus_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    JobStatus
		wantErr bool
	}{
		{"enqueued string", `"enqueued"`, StatusEnqueued, false},
		{"running string", `"running"`, StatusRunning, false},
		{"completed string", `"completed"`, StatusCompleted, false},
		{"failed string", `"failed"`, StatusFailed, false},
		{"dead string", `"dead"`, StatusDead, false},
		{"cancelled string", `"cancelled"`, StatusCancelled, false},
		{"fallback integer 0", `0`, StatusEnqueued, false},
		{"fallback integer 1", `1`, StatusRunning, false},
		{"fallback integer 2", `2`, StatusCompleted, false},
		{"fallback integer 3", `3`, StatusFailed, false},
		{"fallback integer 4", `4`, StatusDead, false},
		{"fallback integer 5", `5`, StatusCancelled, false},
		{"fallback integer out of bounds", `99`, StatusEnqueued, false},
		{"arbitrary string", `"super_status"`, JobStatus("super_status"), false},
		{"invalid JSON type", `true`, StatusEnqueued, true},
		{"empty JSON string", `""`, StatusEnqueued, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got JobStatus
			err := json.Unmarshal([]byte(tt.input), &got)
			if (err != nil) != tt.wantErr {
				t.Errorf("JobStatus.UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("JobStatus.UnmarshalJSON() = %v, want %v", got, tt.want)
			}
		})
	}
}
