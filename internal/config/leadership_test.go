package config

import (
	"os"
	"testing"
)

func TestElectionNamespace(t *testing.T) {
	t.Run("POD_NAMESPACE overrides the projected namespace", func(t *testing.T) {
		t.Setenv("POD_NAMESPACE", "explicit-namespace")
		got, err := resolveNamespace(func(string) ([]byte, error) {
			return []byte("pod-namespace\n"), nil
		})
		if err != nil || got != "explicit-namespace" {
			t.Fatalf("resolveNamespace() = %q, %v; want explicit-namespace, nil", got, err)
		}
	})

	t.Run("reads the pod namespace from the service-account projection", func(t *testing.T) {
		t.Setenv("POD_NAMESPACE", "")
		got, err := resolveNamespace(func(string) ([]byte, error) {
			return []byte("  pod-namespace\n"), nil
		})
		if err != nil || got != "pod-namespace" {
			t.Fatalf("resolveNamespace() = %q, %v; want pod-namespace, nil", got, err)
		}
	})

	t.Run("missing POD_NAMESPACE and projection is an error", func(t *testing.T) {
		t.Setenv("POD_NAMESPACE", "")
		if _, err := resolveNamespace(func(string) ([]byte, error) {
			return nil, os.ErrNotExist
		}); err == nil {
			t.Fatal("expected an error with no POD_NAMESPACE and no projected namespace")
		}
	})
}
