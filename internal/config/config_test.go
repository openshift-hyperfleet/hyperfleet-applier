package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
)

// TestLoadConfigPrecedence proves CLI flags override env vars, which override YAML.
func TestLoadConfigPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	contents := `management_cluster: file-cluster
poll_interval: 2m
discovery_refresh_interval: 45s
clients:
  redis:
    url: redis://file-redis:6379/0
  kubernetes:
    kube_config_path: /file/kubeconfig
log:
  level: info
  format: json
  output: stdout
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("HYPERFLEET_POLL_INTERVAL", "30s")

	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("management-cluster", "", "")
	if err := flags.Set("management-cluster", "flag-cluster"); err != nil {
		t.Fatalf("set flag: %v", err)
	}

	cfg, err := LoadConfig(path, flags)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.ManagementCluster != "flag-cluster" {
		t.Errorf("ManagementCluster = %q, want flag-cluster", cfg.ManagementCluster)
	}
	if cfg.PollInterval != 30*time.Second {
		t.Errorf("PollInterval = %s, want 30s", cfg.PollInterval)
	}
	if cfg.DiscoveryRefreshInterval != 45*time.Second {
		t.Errorf("DiscoveryRefreshInterval = %s, want 45s", cfg.DiscoveryRefreshInterval)
	}
	if cfg.Clients.Redis.URL != "redis://file-redis:6379/0" {
		t.Errorf("Redis URL = %q, want file value", cfg.Clients.Redis.URL)
	}
}

// TestLoadConfigRejectsUnknownFields proves extra YAML keys are a load error.
func TestLoadConfigRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("management_cluster: cluster\nunknown: true\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := LoadConfig(path, nil)
	if err == nil || !strings.Contains(err.Error(), "invalid keys") {
		t.Fatalf("LoadConfig() error = %v, want unknown-field error", err)
	}
}

// TestLoadConfigRejectsOmittedRequired proves that required fields cause
// validation errors when omitted — the Go binary has no hidden defaults.
func TestLoadConfigRejectsOmittedRequired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("management_cluster: cluster\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := LoadConfig(path, nil)
	if err == nil {
		t.Fatal("LoadConfig() should fail when required fields are omitted")
	}
}

// TestRedactedHidesRedisPassword proves config dumps replace the Redis password.
func TestRedactedHidesRedisPassword(t *testing.T) {
	cfg := &Config{}
	cfg.Clients.Redis.URL = "rediss://user:secret@redis.example.com:6379/0"

	redacted := cfg.Redacted()
	if strings.Contains(redacted.Clients.Redis.URL, "secret") {
		t.Errorf("redacted Redis URL exposes password: %q", redacted.Clients.Redis.URL)
	}
	if !strings.Contains(redacted.Clients.Redis.URL, "REDACTED") {
		t.Errorf("redacted Redis URL = %q, want REDACTED", redacted.Clients.Redis.URL)
	}
	if !strings.Contains(cfg.Clients.Redis.URL, "secret") {
		t.Errorf("Redacted() mutated original URL: %q", cfg.Clients.Redis.URL)
	}
}

// TestRedactSecretsRemovesURLPasswords covers wrapped errors and username-only URLs.
func TestRedactSecretsRemovesURLPasswords(t *testing.T) {
	got := RedactSecrets("parse Redis URL: redis://user:supersecret@host:6379/0")
	if strings.Contains(got, "supersecret") {
		t.Errorf("RedactSecrets leaked password: %q", got)
	}
	if !strings.Contains(got, "redis://user:REDACTED@host:6379/0") {
		t.Errorf("RedactSecrets = %q, want redacted userinfo", got)
	}

	plain := "redis://localhost:6379/0"
	if RedactSecrets(plain) != plain {
		t.Errorf("RedactSecrets(%q) = %q, want unchanged", plain, RedactSecrets(plain))
	}
}
