package config

import (
	"os"
	"testing"
)

func TestElectionNamespace(t *testing.T) {
	tests := []struct {
		name         string
		envNamespace string
		want         string
		projection   []byte // nil means the namespace file is absent
		wantErr      bool
	}{
		{
			name:         "POD_NAMESPACE overrides the projected namespace",
			envNamespace: "explicit-namespace",
			want:         "explicit-namespace",
			projection:   []byte("pod-namespace\n"),
		},
		{
			name:       "reads the pod namespace from the service-account projection",
			want:       "pod-namespace",
			projection: []byte("  pod-namespace\n"),
		},
		{
			name:       "empty projection and no POD_NAMESPACE is an error",
			projection: []byte("\n"),
			wantErr:    true,
		},
		{
			name:    "missing projection and no POD_NAMESPACE is an error",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("POD_NAMESPACE", tt.envNamespace)
			got, err := resolveNamespace(func(string) ([]byte, error) {
				if tt.projection == nil {
					return nil, os.ErrNotExist
				}
				return tt.projection, nil
			})
			if tt.wantErr {
				if err == nil {
					t.Fatal("resolveNamespace() error = nil, want an error")
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("resolveNamespace() = %q, %v; want %q, nil", got, err, tt.want)
			}
		})
	}
}
