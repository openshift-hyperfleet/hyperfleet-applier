package leadership

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// TestReleaseLeaseHandsOver covers the hand-over record: a deliberate exit
// writes an already-expired Lease (empty holder, one-second duration) while
// this replica is still the recorded holder, and leaves the record alone
// when it is not.
func TestReleaseLeaseHandsOver(t *testing.T) {
	const partition = "handover-partition"
	client := fake.NewClientset()
	started := make(chan struct{})
	r, err := New(client.CoordinationV1(), testNamespace, partition, "replica",
		funcRunnable(func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	fastElection(r)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("controller never started")
	}

	// Graceful shutdown drains the controller and hands the Lease over.
	cancel()
	select {
	case shutdownErr := <-done:
		if shutdownErr != nil {
			t.Fatalf("Start = %v, want nil on graceful shutdown", shutdownErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after shutdown")
	}
	lease, err := client.CoordinationV1().Leases(testNamespace).Get(
		t.Context(), LeaseName(partition), metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "" {
		t.Fatalf("holder after hand-over = %v, want empty", lease.Spec.HolderIdentity)
	}
	if lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds != handOverLeaseDurationSeconds {
		t.Fatalf("duration after hand-over = %v, want %d", lease.Spec.LeaseDurationSeconds, handOverLeaseDurationSeconds)
	}
}

// TestReleaseLeaseSkipsForeignHolder pins that the hand-over never touches
// a Lease recorded to another identity.
func TestReleaseLeaseSkipsForeignHolder(t *testing.T) {
	const partition = "foreign-holder-partition"
	client := fake.NewClientset()
	r, err := New(client.CoordinationV1(), testNamespace, partition, "replica")
	if err != nil {
		t.Fatal(err)
	}
	// Acquire once so everSucceeded does not short-circuit, then record
	// another holder, as after a takeover.
	fastElection(r)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	started := make(chan struct{})
	r2, err := New(client.CoordinationV1(), testNamespace, partition, "replica",
		funcRunnable(func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	fastElection(r2)
	done := make(chan error, 1)
	go func() { done <- r2.Start(ctx) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("controller never started")
	}
	cancel()
	select {
	case shutdownErr := <-done:
		if shutdownErr != nil {
			t.Fatalf("Start = %v, want nil", shutdownErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return")
	}

	lease, err := client.CoordinationV1().Leases(testNamespace).Get(
		t.Context(), LeaseName(partition), metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	now := metav1.NowMicro()
	lease.Spec.HolderIdentity = ptr("another-replica")
	lease.Spec.RenewTime = &now
	if _, updateErr := client.CoordinationV1().Leases(testNamespace).Update(
		t.Context(), lease, metav1.UpdateOptions{},
	); updateErr != nil {
		t.Fatal(updateErr)
	}

	r.releaseLease(t.Context())
	lease, err = client.CoordinationV1().Leases(testNamespace).Get(
		t.Context(), LeaseName(partition), metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "another-replica" {
		t.Fatalf("foreign holder was modified: %v", lease.Spec.HolderIdentity)
	}
}

// TestReleaseLeaseReadFailureKeepsCleanExit covers the hand-over's read
// failure: when the Lease cannot be read during a deliberate exit, Start
// still returns nil on the graceful path and the record keeps the original
// holder and duration, so the successor falls back to the expiry.
func TestReleaseLeaseReadFailureKeepsCleanExit(t *testing.T) {
	const partition = "handover-read-failure-partition"
	client := fake.NewClientset()
	var failLease atomic.Bool
	client.PrependReactor("*", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
		if failLease.Load() {
			return true, nil, errors.New("lease API unavailable")
		}
		return false, nil, nil
	})
	started := make(chan struct{})
	r, err := New(client.CoordinationV1(), testNamespace, partition, "replica",
		funcRunnable(func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	fastElection(r)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("controller never started")
	}

	// The Lease API fails from here on: the hand-over cannot read the
	// record, so the successor must fall back to the expiry.
	failLease.Store(true)
	cancel()
	select {
	case shutdownErr := <-done:
		if shutdownErr != nil {
			t.Fatalf("Start = %v, want nil despite the hand-over read failure", shutdownErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after shutdown")
	}

	failLease.Store(false)
	assertNoHandOver(t, client, partition, "replica")
}

// TestReleaseLeaseUpdateFailureKeepsCleanExit covers the hand-over's write
// failure: the Lease reads fine but the record update fails, so Start still
// returns nil on the graceful path and the record keeps the original holder
// and duration, leaving the successor to the expiry.
func TestReleaseLeaseUpdateFailureKeepsCleanExit(t *testing.T) {
	const partition = "handover-update-failure-partition"
	client := fake.NewClientset()
	var failUpdate atomic.Bool
	client.PrependReactor("update", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
		if failUpdate.Load() {
			return true, nil, errors.New("lease API unavailable")
		}
		return false, nil, nil
	})
	started := make(chan struct{})
	r, err := New(client.CoordinationV1(), testNamespace, partition, "replica",
		funcRunnable(func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	fastElection(r)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("controller never started")
	}

	// Lease reads keep working; only the record update fails, so the
	// hand-over reaches its write and loses it.
	failUpdate.Store(true)
	cancel()
	select {
	case shutdownErr := <-done:
		if shutdownErr != nil {
			t.Fatalf("Start = %v, want nil despite the hand-over update failure", shutdownErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after shutdown")
	}

	assertNoHandOver(t, client, partition, "replica")
}

// assertNoHandOver checks the Lease still records holder with the original
// fastElection duration: no hand-over write landed, so a successor must
// wait out the expiry.
func assertNoHandOver(t *testing.T, client *fake.Clientset, partition, holder string) {
	t.Helper()
	lease, err := client.CoordinationV1().Leases(testNamespace).Get(
		t.Context(), LeaseName(partition), metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != holder {
		t.Fatalf("holder after failed hand-over = %v, want %q", lease.Spec.HolderIdentity, holder)
	}
	// fastElection sets a 3s LeaseDuration; a hand-over would write
	// handOverLeaseDurationSeconds instead.
	if lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds != 3 {
		t.Fatalf("duration after failed hand-over = %v, want the original 3s", lease.Spec.LeaseDurationSeconds)
	}
}

// TestControllerFailureHandsOverLease covers the hand-over on the other
// deliberate exit: a runnable returns an error while the parent context is
// still alive, so Start returns that error and hands the Lease over.
func TestControllerFailureHandsOverLease(t *testing.T) {
	const partition = "handover-controller-failure-partition"
	client := fake.NewClientset()
	want := errors.New("controller failure")
	r, err := New(client.CoordinationV1(), testNamespace, partition, "replica",
		funcRunnable(func(context.Context) error { return want }),
	)
	if err != nil {
		t.Fatal(err)
	}
	fastElection(r)

	done := make(chan error, 1)
	go func() { done <- r.Start(t.Context()) }()
	select {
	case startErr := <-done:
		if !errors.Is(startErr, want) {
			t.Fatalf("Start = %v, want %v", startErr, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not stop")
	}

	lease, err := client.CoordinationV1().Leases(testNamespace).Get(
		t.Context(), LeaseName(partition), metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "" {
		t.Fatalf("holder after hand-over = %v, want empty", lease.Spec.HolderIdentity)
	}
	if lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds != handOverLeaseDurationSeconds {
		t.Fatalf("duration after hand-over = %v, want %d", lease.Spec.LeaseDurationSeconds, handOverLeaseDurationSeconds)
	}
}

// TestCleanShutdownHandsOverLeaseQuickly is the HYPERFLEET-1779 regression
// test: after a graceful shutdown the next replica must take over within
// about one retry period, not by waiting out the Lease expiry. The timings
// separate the two: a 6s Lease makes an expiry-driven takeover impossible
// inside the 2s budget.
func TestCleanShutdownHandsOverLeaseQuickly(t *testing.T) {
	const partition = "handover-quick-partition"
	client := fake.NewClientset()
	aStarted := make(chan struct{})
	a, err := New(client.CoordinationV1(), testNamespace, partition, "replica-a",
		funcRunnable(func(ctx context.Context) error {
			close(aStarted)
			<-ctx.Done()
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	// A 6s Lease with a 3s renew deadline: an expiry-driven takeover needs
	// most of 6s; the hand-over must deliver one in ~100ms.
	a.config.LeaseDuration = 6 * time.Second
	a.config.RenewDeadline = 3 * time.Second
	a.config.RetryPeriod = 100 * time.Millisecond

	bStarted := make(chan struct{})
	b, err := New(client.CoordinationV1(), testNamespace, partition, "replica-b",
		funcRunnable(func(ctx context.Context) error {
			close(bStarted)
			<-ctx.Done()
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	b.config.LeaseDuration = 6 * time.Second
	b.config.RenewDeadline = 3 * time.Second
	b.config.RetryPeriod = 100 * time.Millisecond

	aCtx, aCancel := context.WithCancel(t.Context())
	t.Cleanup(aCancel)
	aDone := make(chan error, 1)
	go func() { aDone <- a.Start(aCtx) }()
	select {
	case <-aStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("replica A never led")
	}

	bCtx, bCancel := context.WithCancel(t.Context())
	t.Cleanup(bCancel)
	bDone := make(chan error, 1)
	go func() { bDone <- b.Start(bCtx) }()

	// Graceful shutdown of the leader (SIGTERM equivalent).
	aCancel()
	select {
	case err := <-aDone:
		if err != nil {
			t.Fatalf("A Start = %v, want nil on graceful shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("A did not shut down")
	}

	// B must lead within a couple of retry periods of the hand-over.
	deadline := time.Now().Add(2 * time.Second)
	select {
	case <-bStarted:
	case <-time.After(time.Until(deadline)):
		t.Fatal("successor did not take over quickly after a clean shutdown")
	}
	bCancel()
	select {
	case err := <-bDone:
		if err != nil {
			t.Fatalf("B Start = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("B did not shut down")
	}
}
