package main

import (
	"strings"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"defaults (simulator, no secret)", nil, ""},
		{"real principal with secret", map[string]string{"PRINCIPAL_URL": "https://principal.example", "CALLBACK_SECRET": "s"}, ""},
		{"real principal without secret", map[string]string{"PRINCIPAL_URL": "https://principal.example"}, "CALLBACK_SECRET is required"},
		{"lease not longer than submit timeout", map[string]string{"SUBMIT_TIMEOUT": "1m", "LEASE_TIMEOUT": "1m"}, "must be greater than"},
		{"invalid duration", map[string]string{"SUBMIT_TIMEOUT": "soon"}, "invalid SUBMIT_TIMEOUT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range []string{"HTTP_ADDR", "DATABASE_URL", "PRINCIPAL_URL", "CALLBACK_SECRET", "SUBMIT_TIMEOUT", "LEASE_TIMEOUT", "RECOVERY_INTERVAL"} {
				t.Setenv(k, tt.env[k])
			}
			_, err := loadConfig()
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}
