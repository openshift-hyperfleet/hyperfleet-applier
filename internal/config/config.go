// Package config loads and validates applier configuration.
package config

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// EnvPrefix is the prefix for environment variables that override applier configuration.
const EnvPrefix = "HYPERFLEET"

// Config is the complete runtime configuration for the applier.
type Config struct {
	Log                      LogConfig     `yaml:"log,omitempty" mapstructure:"log"`
	ManagementCluster        string        `yaml:"management_cluster" mapstructure:"management_cluster"`
	Clients                  ClientsConfig `yaml:"clients" mapstructure:"clients"`
	PollInterval             time.Duration `yaml:"poll_interval" mapstructure:"poll_interval"`
	DiscoveryRefreshInterval time.Duration `yaml:"discovery_refresh_interval" mapstructure:"discovery_refresh_interval"`
	DebugConfig              bool          `yaml:"debug_config,omitempty" mapstructure:"debug_config"`
}

// LogConfig contains logging configuration.
type LogConfig struct {
	Level  string `yaml:"level,omitempty" mapstructure:"level"`
	Format string `yaml:"format,omitempty" mapstructure:"format"`
	Output string `yaml:"output,omitempty" mapstructure:"output"`
}

// ClientsConfig contains external client configuration.
type ClientsConfig struct {
	Redis      RedisConfig      `yaml:"redis" mapstructure:"redis"`
	Kubernetes KubernetesConfig `yaml:"kubernetes" mapstructure:"kubernetes"`
}

// RedisConfig contains Redis connection configuration.
type RedisConfig struct {
	URL string `yaml:"url" mapstructure:"url"`
}

// KubernetesConfig contains Kubernetes client configuration.
type KubernetesConfig struct {
	KubeConfigPath string `yaml:"kube_config_path,omitempty" mapstructure:"kube_config_path"`
}

// newConfig returns a Config with only logging defaults set so the binary
// can emit structured logs even when the config is partially broken.
// All other defaults live in the Helm chart's values.yaml.
func newConfig() *Config {
	return &Config{
		Log: LogConfig{
			Level:  "info",
			Format: "json",
			Output: "stdout",
		},
	}
}

// viperKeyMappings maps config paths to HYPERFLEET_ environment variable suffixes.
// Complex values are intentionally excluded because they cannot be represented as scalars.
var viperKeyMappings = map[string]string{
	"debug_config":                          "DEBUG_CONFIG",
	"management_cluster":                    "MANAGEMENT_CLUSTER",
	"log::level":                            "LOG_LEVEL",
	"log::format":                           "LOG_FORMAT",
	"log::output":                           "LOG_OUTPUT",
	"clients::redis::url":                   "REDIS_URL",
	"clients::kubernetes::kube_config_path": "KUBERNETES_KUBE_CONFIG_PATH",
	"poll_interval":                         "POLL_INTERVAL",
	"discovery_refresh_interval":            "DISCOVERY_REFRESH_INTERVAL",
}

// cliFlags maps command-line flag names to config paths.
var cliFlags = map[string]string{
	"debug-config":                "debug_config",
	"management-cluster":          "management_cluster",
	"log-level":                   "log::level",
	"log-format":                  "log::format",
	"log-output":                  "log::output",
	"redis-url":                   "clients::redis::url",
	"kubernetes-kube-config-path": "clients::kubernetes::kube_config_path",
	"poll-interval":               "poll_interval",
	"discovery-refresh-interval":  "discovery_refresh_interval",
}

// LoadConfig loads configuration with the standard HyperFleet precedence:
// CLI flags > environment variables > YAML file > defaults.
func LoadConfig(configFile string, flags *pflag.FlagSet) (*Config, error) {
	cfg := newConfig()
	if configFile == "" {
		configFile = os.Getenv(EnvPrefix + "_CONFIG")
	}

	v := viper.NewWithOptions(viper.KeyDelimiter("::"))
	if configFile != "" {
		v.SetConfigFile(configFile)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	for configPath, envSuffix := range viperKeyMappings {
		envName := EnvPrefix + "_" + envSuffix
		if err := v.BindEnv(configPath, envName); err != nil {
			return nil, fmt.Errorf("failed to bind env var %s: %w", envName, err)
		}
	}

	if flags != nil {
		for flagName, configPath := range cliFlags {
			if flag := flags.Lookup(flagName); flag != nil && flag.Changed {
				v.Set(configPath, flag.Value.String())
			}
		}
	}

	if err := v.UnmarshalExact(cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}

// Validate validates the merged configuration.
func (c *Config) Validate() error {
	if c.ManagementCluster == "" {
		return fmt.Errorf("management_cluster is required")
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("poll_interval must be positive, got %s", c.PollInterval)
	}
	if c.DiscoveryRefreshInterval <= 0 {
		return fmt.Errorf(
			"discovery_refresh_interval must be positive, got %s",
			c.DiscoveryRefreshInterval,
		)
	}

	redisURL, err := url.Parse(c.Clients.Redis.URL)
	if err != nil {
		return fmt.Errorf("clients.redis.url is invalid: %s", RedactSecrets(err.Error()))
	}
	if redisURL.Scheme != "redis" && redisURL.Scheme != "rediss" && redisURL.Scheme != "unix" {
		return fmt.Errorf("clients.redis.url must use redis, rediss, or unix scheme")
	}
	if redisURL.Scheme != "unix" && redisURL.Host == "" {
		return fmt.Errorf("clients.redis.url must include a host")
	}
	if c.Log.Level == "" {
		return fmt.Errorf("log.level is required")
	}
	if c.Log.Format == "" {
		return fmt.Errorf("log.format is required")
	}
	if c.Log.Output == "" {
		return fmt.Errorf("log.output is required")
	}
	return nil
}

// urlPasswordPattern matches the password portion of a URL's userinfo.
var urlPasswordPattern = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://[^:@\s/]*:)[^@\s]+@`)

// RedactSecrets returns s with URL passwords replaced by REDACTED so command
// errors and logs cannot leak Redis or other endpoint credentials.
func RedactSecrets(s string) string {
	return urlPasswordPattern.ReplaceAllString(s, "${1}REDACTED@")
}

// Redacted returns a copy safe for config dumps and logs.
func (c *Config) Redacted() *Config {
	copy := *c
	copy.Clients.Redis.URL = RedactSecrets(copy.Clients.Redis.URL)
	return &copy
}
