package leadership

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

type funcRunnable func(context.Context) error

const (
	testNamespace       = "default"
	configMapResource   = "configmaps"
	configMapKind       = "ConfigMap"
	testOwner           = "test-owner"
	testApplyTargetName = "target"
	testDeleteTarget    = "delete-target"
)

func (f funcRunnable) Start(ctx context.Context) error { return f(ctx) }

func fastElection(r *Runnable) {
	r.config.LeaseDuration = 3 * time.Second
	r.config.RenewDeadline = time.Second
	r.config.RetryPeriod = 100 * time.Millisecond
}

func TestLeaseNameAndTiming(t *testing.T) {
	name := LeaseName("arbitrary/UPPERCASE partition")
	if errs := validation.IsDNS1123Subdomain(name); len(errs) != 0 {
		t.Fatalf("invalid Lease name: %v", errs)
	}
	if name != LeaseName("arbitrary/UPPERCASE partition") || name == LeaseName("other") {
		t.Fatal("Lease name must be stable and partition-specific")
	}
	if LeaseDuration-RenewDeadline <= 30*time.Second || RenewDeadline <= RetryPeriod {
		t.Fatal("election timings must leave margin beyond cluster-write timeouts")
	}
}

func TestLeadershipLossCancelsAndWaitsForControllers(t *testing.T) {
	client := fake.NewClientset()
	var fail atomic.Bool
	client.PrependReactor("*", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
		if fail.Load() {
			return true, nil, errors.New("lease API unavailable")
		}
		return false, nil, nil
	})
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseController := func() { releaseOnce.Do(func() { close(release) }) }
	r, err := New(client.CoordinationV1(), testNamespace, "test", "replica", funcRunnable(func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	fastElection(r)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	t.Cleanup(releaseController)
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()
	awaitSignal(t, started)
	fail.Store(true)
	awaitSignal(t, canceled)
	select {
	case err := <-done:
		t.Fatalf("returned before controllers stopped: %v", err)
	default:
	}
	releaseController()
	select {
	case err := <-done:
		if !errors.Is(err, ErrLeadershipLost) {
			t.Fatalf("Start = %v, want leadership loss", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not wait for controller shutdown")
	}
}

func TestControllerFailureStopsElectionAndSiblings(t *testing.T) {
	client := fake.NewClientset()
	want := errors.New("controller failure")
	siblingStopped := make(chan struct{})
	r, err := New(client.CoordinationV1(), testNamespace, "test", "replica",
		funcRunnable(func(context.Context) error { return want }),
		funcRunnable(func(ctx context.Context) error {
			<-ctx.Done()
			close(siblingStopped)
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	fastElection(r)
	done := make(chan error, 1)
	go func() { done <- r.Start(t.Context()) }()
	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatalf("Start = %v, want %v", err, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not stop")
	}
	awaitSignal(t, siblingStopped)
}

func TestCanceledStandbyDoesNotStartControllers(t *testing.T) {
	r, err := New(fake.NewClientset().CoordinationV1(), testNamespace, "test", "replica",
		funcRunnable(func(context.Context) error {
			t.Error("canceled standby started controller")
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
}

func awaitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(8 * time.Second):
		t.Fatal("timed out waiting for controller")
	}
}
