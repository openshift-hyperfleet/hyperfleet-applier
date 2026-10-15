package leadership

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
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
	if lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds != 1 {
		t.Fatalf("duration after hand-over = %v, want 1s", lease.Spec.LeaseDurationSeconds)
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

	r.releaseLease()
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
