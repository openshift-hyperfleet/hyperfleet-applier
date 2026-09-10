package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	hflog "github.com/openshift-hyperfleet/hyperfleet-logger"
	redisclient "github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/discovery"
	discoverycache "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/config"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/applydesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/deletedesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/readdesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/reconciler"
	desireredis "github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/redis"
)

var (
	version string
	commit  string
	date    string
)

func main() {
	if err := newRootCommand().ExecuteContext(context.Background()); err != nil {
		command := strings.Join(os.Args[1:], " ")
		if command == "" {
			command = "(no command)"
		}
		fmt.Fprintf(
			os.Stderr,
			"Error executing command 'applier %s': %s\n",
			config.RedactSecrets(command),
			config.RedactSecrets(err.Error()),
		)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:   "applier",
		Short: "HyperFleet Applier - reconcile Kubernetes resource desires",
		Long: `HyperFleet Applier reconciles ApplyDesires, DeleteDesires, and
ReadDesires from a shared store against a Kubernetes management cluster.`,
		SilenceErrors: true,
	}
	root.CompletionOptions.DisableDefaultCmd = true
	root.AddCommand(newServeCommand(), newConfigDumpCommand(), newVersionCommand())
	return root
}

func newServeCommand() *cobra.Command {
	var configFile string
	cmd := &cobra.Command{
		Use:          "serve",
		Short:        "Start the applier reconciliation service",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.LoadConfig(configFile, cmd.Flags())
			if err != nil {
				return err
			}
			if err := initLogging(&cfg.Log); err != nil {
				return fmt.Errorf("initialize logging: %w", err)
			}
			if cfg.DebugConfig {
				data, marshalErr := yaml.Marshal(cfg.Redacted())
				if marshalErr != nil {
					slog.WarnContext(cmd.Context(), "marshal configuration for debug logging", "error", marshalErr)
				} else {
					slog.InfoContext(cmd.Context(), "loaded configuration", "config", string(data))
				}
			}
			return runServe(cmd.Context(), cfg)
		},
	}
	cmd.Flags().StringVarP(&configFile, "config", "c", "", "Path to configuration file (YAML)")
	addConfigOverrideFlags(cmd)
	return cmd
}

func newConfigDumpCommand() *cobra.Command {
	var configFile string
	cmd := &cobra.Command{
		Use:           "config-dump",
		Short:         "Load and print the merged applier configuration as YAML",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.LoadConfig(configFile, cmd.Flags())
			if err != nil {
				return err
			}
			data, err := yaml.Marshal(cfg.Redacted())
			if err != nil {
				return fmt.Errorf("marshal configuration: %w", err)
			}
			fmt.Print(string(data))
			return nil
		},
	}
	cmd.Flags().StringVarP(&configFile, "config", "c", "", "Path to configuration file (YAML)")
	addConfigOverrideFlags(cmd)
	return cmd
}

func addConfigOverrideFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("debug-config", false, "Log the merged configuration. Env: HYPERFLEET_DEBUG_CONFIG")
	cmd.Flags().String(
		"management-cluster", "", "Management-cluster partition. Env: HYPERFLEET_MANAGEMENT_CLUSTER",
	)
	cmd.Flags().StringP("log-level", "l", "", "Log level. Env: HYPERFLEET_LOG_LEVEL")
	cmd.Flags().StringP("log-format", "f", "", "Log format. Env: HYPERFLEET_LOG_FORMAT")
	cmd.Flags().String("log-output", "", "Log output. Env: HYPERFLEET_LOG_OUTPUT")
	cmd.Flags().String("redis-url", "", "Redis connection URL. Env: HYPERFLEET_REDIS_URL")
	cmd.Flags().String(
		"kubernetes-kube-config-path", "",
		"Path to kubeconfig; empty uses in-cluster auth. Env: HYPERFLEET_KUBERNETES_KUBE_CONFIG_PATH",
	)
	cmd.Flags().String("poll-interval", "", "Controller poll interval. Env: HYPERFLEET_POLL_INTERVAL")
	cmd.Flags().String(
		"discovery-refresh-interval", "",
		"REST discovery refresh interval. Env: HYPERFLEET_DISCOVERY_REFRESH_INTERVAL",
	)
}

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Printf("Version:    %s\n", version)
			fmt.Printf("Commit:     %s\n", commit)
			fmt.Printf("Build Date: %s\n", date)
		},
	}
}

// initLogging installs the process-wide slog handler from the merged log configuration.
func initLogging(logConfig *config.LogConfig) error {
	level, err := hflog.ParseLevel(logConfig.Level)
	if err != nil {
		return fmt.Errorf("parse log level: %w", err)
	}
	format, err := hflog.ParseFormat(logConfig.Format)
	if err != nil {
		return fmt.Errorf("parse log format: %w", err)
	}
	output, err := hflog.ParseOutput(logConfig.Output)
	if err != nil {
		return fmt.Errorf("parse log output: %w", err)
	}
	handler := hflog.NewHandler(
		"applier",
		version,
		hflog.WithLevel(level),
		hflog.WithFormat(format),
		hflog.WithOutput(output),
	)
	slog.SetDefault(slog.New(handler))
	return nil
}

// runServe constructs Kubernetes and Redis clients and runs every reconciler until shutdown.
func runServe(parent context.Context, cfg *config.Config) error {
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	dyn, mapper, err := newKubernetesClients(cfg.Clients.Kubernetes.KubeConfigPath)
	if err != nil {
		return err
	}

	store, closeRedis, err := newRedisStore(ctx, cfg.Clients.Redis.URL)
	if err != nil {
		return err
	}
	defer closeRedis()

	pollInterval := cfg.PollInterval

	runnables := []reconciler.Runnable{
		newDiscoveryRefresher(mapper, cfg.DiscoveryRefreshInterval),
		applydesire.New(store, store, dyn, mapper, cfg.ManagementCluster, pollInterval),
		deletedesire.New(store, store, dyn, mapper, cfg.ManagementCluster, pollInterval),
		readdesire.New(store, store, dyn, mapper, cfg.ManagementCluster, pollInterval),
	}

	slog.InfoContext(ctx,
		"starting HyperFleet Applier",
		"version", version,
		"commit", commit,
		"management_cluster", cfg.ManagementCluster,
	)
	err = startRunnables(ctx, runnables)
	if err == nil {
		slog.Info("HyperFleet Applier stopped gracefully")
	}
	return err
}

func newKubernetesClients(kubeConfigPath string) (dynamic.Interface, meta.ResettableRESTMapper, error) {
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	if err != nil {
		return nil, nil, fmt.Errorf("build Kubernetes client config: %w", err)
	}
	dyn, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("build Kubernetes dynamic client: %w", err)
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("build Kubernetes discovery client: %w", err)
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(discoverycache.NewMemCacheClient(discoveryClient))
	return dyn, mapper, nil
}

func newRedisStore(ctx context.Context, redisURL string) (*desireredis.Store, func(), error) {
	redisOptions, err := redisclient.ParseURL(redisURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse Redis URL: %w", err)
	}
	redisClient := redisclient.NewClient(redisOptions)
	cleanup := func() {
		if closeErr := redisClient.Close(); closeErr != nil {
			slog.Error("close Redis client", "error", closeErr)
		}
	}
	if pingErr := redisClient.Ping(ctx).Err(); pingErr != nil {
		cleanup()
		return nil, nil, fmt.Errorf("connect to Redis: %w", pingErr)
	}
	return desireredis.New(redisClient), cleanup, nil
}

type discoveryRefresher struct {
	mapper   meta.ResettableRESTMapper
	interval time.Duration
}

func newDiscoveryRefresher(mapper meta.ResettableRESTMapper, interval time.Duration) *discoveryRefresher {
	return &discoveryRefresher{mapper: mapper, interval: interval}
}

// Start invalidates cached discovery immediately and periodically so newly
// installed resource types become visible without restarting the applier.
func (r *discoveryRefresher) Start(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		r.mapper.Reset()
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// startRunnables starts every controller concurrently and waits for all of
// them to stop. Any runnable exit cancels its siblings so the parent wait
// unblocks; a non-nil return is forwarded. A caller-driven cancellation
// remains a clean shutdown.
func startRunnables(ctx context.Context, runnables []reconciler.Runnable) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, len(runnables))
	var wg sync.WaitGroup
	for _, runnable := range runnables {
		wg.Go(func() {
			err := runnable.Start(runCtx)
			if err != nil {
				slog.Error("runnable stopped", "error", err)
				errCh <- err
			} else {
				slog.Info("runnable stopped")
			}
			cancel()
		})
	}

	<-runCtx.Done()
	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
