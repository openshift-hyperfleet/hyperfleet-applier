// Package main is the entry point for the hyperfleet-applier binary.
// The applier runs on target clusters, polls desires from a store,
// reconciles them against the local kube-apiserver, and writes status back.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/applydesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/deletedesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/readdesire"
	redisstore "github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/redis"
)

// TODO: HYPERFLEET-1521 - parallel reconciler execution
// Currently main is temporary entry point
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	redisAddr := requiredEnv("REDIS_ADDRESS")
	managementCluster := requiredEnv("MANAGEMENT_CLUSTER")
	pollIntervalStr := requiredEnv("POLL_INTERVAL")

	pollInterval, err := time.ParseDuration(pollIntervalStr)
	if err != nil {
		slog.Error("invalid poll interval", "value", pollIntervalStr, "error", err)
		os.Exit(1)
	}

	slog.Info("starting hyperfleet-applier",
		"redis", redisAddr,
		"managementCluster", managementCluster,
		"pollInterval", pollInterval,
	)

	kubeConfig, err := getKubeConfig()
	if err != nil {
		slog.Error("failed to get kube config", "error", err)
		os.Exit(1)
	}

	dynClient, err := dynamic.NewForConfig(kubeConfig)
	if err != nil {
		slog.Error("failed to create dynamic client", "error", err)
		os.Exit(1)
	}

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(kubeConfig)
	if err != nil {
		slog.Error("failed to create discovery client", "error", err)
		os.Exit(1)
	}

	redisClient := verifyRedisClient(redisAddr)
	if redisClient == nil {
		slog.Error("failed to connect to redis")
		os.Exit(1)
	}

	defer func() {
		if closeErr := redisClient.Close(); closeErr != nil {
			slog.Error("failed to close redis client", "error", closeErr)
		}
	}()

	store := redisstore.New(redisClient)

	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient))

	// Create reconcilers
	applyReconciler := applydesire.New(store, store, dynClient, mapper, managementCluster)
	deleteReconciler := deletedesire.New(store, store, dynClient, mapper, managementCluster)
	readController := readdesire.NewController(store, store, dynClient, mapper, managementCluster, pollInterval)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		slog.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	// Start the read controller in a goroutine (it blocks)
	go func() {
		if err := readController.Run(ctx); err != nil {
			slog.Error("read controller exited with error", "error", err)
			cancel()
		}
	}()

	// TODO: HYPERFLEET-1521 - Future work to make all reconcilers parallel and consistent
	// Run the reconciliation loop for apply and delete desires
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	slog.Info("starting reconciliation loop")
	for {
		select {
		case <-ctx.Done():
			slog.Info("context canceled, shutting down")
			return
		case <-ticker.C:
			// Reconcile apply desires
			if err := applyReconciler.ReconcileAll(ctx); err != nil {
				if ctx.Err() != nil {
					slog.Info("apply reconciliation canceled")
					return
				}
				slog.Error("apply reconciliation failed", "error", err)
			}

			// Reconcile delete desires
			if err := deleteReconciler.ReconcileAll(ctx); err != nil {
				if ctx.Err() != nil {
					slog.Info("delete reconciliation canceled")
					return
				}
				slog.Error("delete reconciliation failed", "error", err)
			}
		}
	}
}

func requiredEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required environment variable not set", "key", key)
		os.Exit(1)
	}
	return v
}

// getKubeConfig returns the Kubernetes configuration.
// It first attempts to use in-cluster config, falling back to kubeconfig file.
func getKubeConfig() (*rest.Config, error) {
	// Try in-cluster config first (when running in a pod)
	config, err := rest.InClusterConfig()
	if err == nil {
		return config, nil
	}

	// Fall back to kubeconfig file (for local development)
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
	return kubeConfig.ClientConfig()
}

func verifyRedisClient(redisAddr string) *redis.Client {
	// Create Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// Verify Redis connection
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if pingErr := redisClient.Ping(ctx).Err(); pingErr != nil {
		slog.Error("failed to ping redis", "error", pingErr)
		return nil
	}

	slog.Info("connected to redis")
	return redisClient
}
