package leadership

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// launchReplica starts r under a context derived from the test's, returning
// the channel Start reports on and the shutdown cancel.
func launchReplica(t *testing.T, r *Runnable) (<-chan error, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()
	return done, cancel
}

// startReplica launches r and waits for its controller to start.
func startReplica(t *testing.T, r *Runnable, started <-chan struct{}) (<-chan error, context.CancelFunc) {
	t.Helper()
	done, cancel := launchReplica(t, r)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("controller never started")
	}
	return done, cancel
}

// awaitStartReturn waits for Start to return and reports its result.
func awaitStartReturn(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return")
		return nil
	}
}

// TestReleaseLeaseHandsOver covers the hand-over record: a deliberate exit
// writes an already-expired Lease (empty holder, one-second duration, the
// recorded transition count carried over) while this replica is still the
// recorded holder, and leaves the record alone when it is not.
func TestReleaseLeaseHandsOver(t *testing.T) {
	const partition = "handover-partition"
	client := fake.NewClientset()
	// A pre-existing expired record from another holder with a transition
	// count: client-go bumps the count when a different identity acquires,
	// and the hand-over must carry the bumped value over instead of
	// resetting it.
	seededTransitions := int32(5)
	oneSecond := int32(1)
	expired := metav1.NewMicroTime(time.Now().Add(-10 * time.Second))
	if _, err := client.CoordinationV1().Leases(testNamespace).Create(t.Context(), &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: LeaseName(partition)},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptr("previous-holder"),
			LeaseDurationSeconds: &oneSecond,
			LeaseTransitions:     &seededTransitions,
			RenewTime:            &expired,
			AcquireTime:          &expired,
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
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
	done, cancel := startReplica(t, r, started)

	// Graceful shutdown drains the controller and hands the Lease over.
	cancel()
	if shutdownErr := awaitStartReturn(t, done); shutdownErr != nil {
		t.Fatalf("Start = %v, want nil on graceful shutdown", shutdownErr)
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
	// A nil LeaseTransitions reports as -1, so the failure message never
	// dereferences the field.
	transitions := int32(-1)
	if lease.Spec.LeaseTransitions != nil {
		transitions = *lease.Spec.LeaseTransitions
	}
	if transitions != seededTransitions+1 {
		t.Fatalf("transitions after hand-over = %d, want %d", transitions, seededTransitions+1)
	}
}

// TestReleaseLeaseSkipsForeignHolder pins that the hand-over never touches
// a Lease recorded to another identity.
func TestReleaseLeaseSkipsForeignHolder(t *testing.T) {
	const partition = "foreign-holder-partition"
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
	// Lead once so everSucceeded stays set, then record another holder, as
	// after a takeover: the hand-over must read the foreign record and
	// leave it alone.
	done, cancel := startReplica(t, r, started)
	cancel()
	if shutdownErr := awaitStartReturn(t, done); shutdownErr != nil {
		t.Fatalf("Start = %v, want nil", shutdownErr)
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
	done, cancel := startReplica(t, r, started)

	// The Lease API fails from here on: the hand-over cannot read the
	// record, so the successor must fall back to the expiry.
	failLease.Store(true)
	cancel()
	if shutdownErr := awaitStartReturn(t, done); shutdownErr != nil {
		t.Fatalf("Start = %v, want nil despite the hand-over read failure", shutdownErr)
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
	done, cancel := startReplica(t, r, started)

	// Lease reads keep working; only the record update fails, so the
	// hand-over reaches its write and loses it.
	failUpdate.Store(true)
	cancel()
	if shutdownErr := awaitStartReturn(t, done); shutdownErr != nil {
		t.Fatalf("Start = %v, want nil despite the hand-over update failure", shutdownErr)
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
	done, _ := launchReplica(t, r)

	if startErr := awaitStartReturn(t, done); !errors.Is(startErr, want) {
		t.Fatalf("Start = %v, want %v", startErr, want)
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

	aDone, aCancel := startReplica(t, a, aStarted)

	// B waits on standby; its two-second takeover budget below is the
	// regression assertion, so it keeps a custom wait instead of the
	// generic startReplica one.
	bDone, bCancel := launchReplica(t, b)

	// Graceful shutdown of the leader (SIGTERM equivalent).
	aCancel()
	if err := awaitStartReturn(t, aDone); err != nil {
		t.Fatalf("A Start = %v, want nil on graceful shutdown", err)
	}

	// B must lead within a couple of retry periods of the hand-over.
	deadline := time.Now().Add(2 * time.Second)
	select {
	case <-bStarted:
	case <-time.After(time.Until(deadline)):
		t.Fatal("successor did not take over quickly after a clean shutdown")
	}
	bCancel()
	if err := awaitStartReturn(t, bDone); err != nil {
		t.Fatalf("B Start = %v, want nil", err)
	}
}
