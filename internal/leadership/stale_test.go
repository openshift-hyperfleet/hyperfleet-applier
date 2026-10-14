package leadership

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/applydesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/deletedesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/serial"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/reconciler"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/memory"
)

// TestStaleGateDeniesAndCancels covers the gate standalone: fresh allows,
// stale denies and cancels, and a never-successful lock reads as stale.
func TestStaleGateDeniesAndCancels(t *testing.T) {
	lock := &trackingLock{}
	lock.markSuccess(time.Now())
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	gate := &staleGate{lock: lock, bound: time.Minute, cancel: cancel}

	if err := gate.CanWrite(); err != nil {
		t.Fatalf("fresh gate denied: %v", err)
	}

	lock.backdateLastSuccess(2 * time.Minute)
	err := gate.CanWrite()
	if !errors.Is(err, reconciler.ErrLeadershipStale) {
		t.Fatalf("stale gate error = %v, want ErrLeadershipStale", err)
	}
	if ctx.Err() == nil {
		t.Fatal("stale gate did not cancel the run context")
	}

	// A never-successful lock reads as stale, not as fresh.
	emptyGate := &staleGate{lock: &trackingLock{}, bound: time.Minute}
	if err := emptyGate.CanWrite(); !errors.Is(err, reconciler.ErrLeadershipStale) {
		t.Fatalf("never-successful lock error = %v, want ErrLeadershipStale", err)
	}
}

// TestCanWriteWithoutGate pins that contexts without a gate always allow
// writes, so controllers used outside the leadership wiring are unaffected.
func TestCanWriteWithoutGate(t *testing.T) {
	if err := reconciler.CanWrite(t.Context()); err != nil {
		t.Fatalf("context without gate denied writes: %v", err)
	}
}

// TestStaleGateBlocksFirstWriteAfterResume pins the first write after a
// resume: with a stale gate in the context, the very first SSA and DELETE
// attempts of a pass are denied before any request leaves the process.
func TestStaleGateBlocksFirstWriteAfterResume(t *testing.T) {
	const partition = "stale-first-write"
	var writes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"target","namespace":"default"}}`)
			return
		}
		writes.Add(1)
	}))
	t.Cleanup(server.Close)
	dyn, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Version: "v1"}})
	mapper.Add(schema.GroupVersionKind{Version: "v1", Kind: configMapKind}, meta.RESTScopeNamespace)

	store := memory.New()
	applyID := desire.Identity{ManagementCluster: partition, Type: desire.TypeApply,
		Resource: configMapResource, Namespace: testNamespace, Name: testApplyTargetName}
	deleteID := applyID
	deleteID.Type, deleteID.Name = desire.TypeDelete, testDeleteTarget
	content := json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap",
		"metadata":{"name":"target","namespace":"default"},"data":{"key":"value"}}`)
	if _, err = store.CreateApplyDesire(t.Context(), desire.ApplyDesire{
		Identity: applyID, Owner: testOwner, Spec: desire.ApplySpec{KubeContent: content},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.CreateDeleteDesire(t.Context(), desire.DeleteDesire{
		Identity: deleteID, Owner: testOwner,
	}); err != nil {
		t.Fatal(err)
	}

	// A leader that renewed long before being stalled: the recorded success
	// is far past the freshness bound. Each pass gets its own gate, since
	// the gate cancels its context on the first denial.
	staleCtx := func() context.Context {
		lock := &trackingLock{}
		lock.backdateLastSuccess(time.Hour)
		gateCtx, gateCancel := context.WithCancel(t.Context())
		t.Cleanup(gateCancel)
		return reconciler.WithLeaderGate(gateCtx, &staleGate{lock: lock, bound: time.Minute, cancel: gateCancel})
	}

	applyErr := applydesire.New(store, store, dyn, mapper, partition).RunOnce(staleCtx())
	if !errors.Is(applyErr, reconciler.ErrLeadershipStale) {
		t.Fatalf("apply RunOnce error = %v, want ErrLeadershipStale", applyErr)
	}
	deleteErr := deletedesire.New(store, store, dyn, mapper, partition).RunOnce(staleCtx())
	if !errors.Is(deleteErr, reconciler.ErrLeadershipStale) {
		t.Fatalf("delete RunOnce error = %v, want ErrLeadershipStale", deleteErr)
	}
	if writes.Load() != 0 {
		t.Fatalf("cluster writes with stale leadership = %d, want 0", writes.Load())
	}
}

// TestStaleLeadershipBlocksWritesInsideRenewDeadline is the HYPERFLEET-1777
// regression test: a leader that renewed, then stalled past the Lease
// duration while another replica took over, must issue no SSA or DELETE once
// it resumes and must exit gate-driven, well inside the RenewDeadline that
// client-go would otherwise make it wait out.
func TestStaleLeadershipBlocksWritesInsideRenewDeadline(t *testing.T) {
	const partition = "stale-partition"
	var writes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"target","namespace":"default"}}`)
			return
		}
		writes.Add(1)
	}))
	t.Cleanup(server.Close)
	dyn, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Version: "v1"}})
	mapper.Add(schema.GroupVersionKind{Version: "v1", Kind: configMapKind}, meta.RESTScopeNamespace)

	store := memory.New()
	applyID := desire.Identity{ManagementCluster: partition, Type: desire.TypeApply,
		Resource: configMapResource, Namespace: testNamespace, Name: testApplyTargetName}
	deleteID := applyID
	deleteID.Type, deleteID.Name = desire.TypeDelete, testDeleteTarget
	content := json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap",
		"metadata":{"name":"target","namespace":"default"},"data":{"key":"value"}}`)
	if _, err = store.CreateApplyDesire(t.Context(), desire.ApplyDesire{
		Identity: applyID, Owner: testOwner, Spec: desire.ApplySpec{KubeContent: content},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.CreateDeleteDesire(t.Context(), desire.DeleteDesire{
		Identity: deleteID, Owner: testOwner,
	}); err != nil {
		t.Fatal(err)
	}

	client := fake.NewClientset()
	loop := serial.New(50*time.Millisecond,
		applydesire.New(store, store, dyn, mapper, partition),
		deletedesire.New(store, store, dyn, mapper, partition))
	r, err := New(client.CoordinationV1(), testNamespace, partition, "replica", loop)
	if err != nil {
		t.Fatal(err)
	}
	// A RenewDeadline close to the LeaseDuration keeps the elector inside its
	// renew poll for ~10s while the gate has already gone stale at ~11.95s;
	// that gap is the bug's window.
	r.config.LeaseDuration = 12 * time.Second
	r.config.RenewDeadline = 10 * time.Second
	r.config.RetryPeriod = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()

	// Wait until the leader is actively reconciling (renewed and writing).
	deadline := time.Now().Add(5 * time.Second)
	for writes.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("leader never issued a write")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Simulate the stall-then-takeover: another identity now holds a fresh,
	// valid Lease record (so the elector keeps failing to renew without
	// giving up), and our last successful renewal is older than the
	// freshness bound, as after a resume.
	lease, err := client.CoordinationV1().Leases(testNamespace).Get(
		t.Context(), LeaseName(partition), metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	now := metav1.NowMicro()
	lease.Spec.HolderIdentity = ptr("another-replica")
	lease.Spec.RenewTime = &now
	lease.Spec.AcquireTime = &now
	if _, err = client.CoordinationV1().Leases(testNamespace).Update(
		t.Context(), lease, metav1.UpdateOptions{},
	); err != nil {
		t.Fatal(err)
	}
	r.lock.backdateLastSuccess(15 * time.Second)

	// From here on no SSA or DELETE may reach the cluster, and the process
	// must exit gate-driven. A deadline-driven exit would take the full
	// RenewDeadline (10s); the gate cancels within a couple of watcher ticks.
	before := writes.Load()
	staleSince := time.Now()
	select {
	case err := <-done:
		if !errors.Is(err, ErrLeadershipLost) {
			t.Fatalf("Start = %v, want ErrLeadershipLost", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("former leader did not exit after going stale")
	}
	if elapsed := time.Since(staleSince); elapsed >= r.config.RenewDeadline {
		t.Fatalf("exit was deadline-driven (%s), want gate-driven", elapsed)
	}
	if got := writes.Load(); got != before {
		t.Fatalf("cluster writes while leadership stale = %d, want %d", got, before)
	}
}

// ptr returns a pointer to s; a test-local replacement for k8s.io/utils/ptr.
func ptr(s string) *string { return &s }

// delayedLock is a resourcelock whose Update succeeds only after a delay
// longer than the caller's freshness bound, so the response is handled well
// after the request was issued.
type delayedLock struct {
	updateDelay time.Duration
}

func (d *delayedLock) Get(context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	return nil, nil, nil
}

func (d *delayedLock) Create(context.Context, resourcelock.LeaderElectionRecord) error {
	return nil
}

func (d *delayedLock) Update(context.Context, resourcelock.LeaderElectionRecord) error {
	time.Sleep(d.updateDelay)
	return nil
}

func (d *delayedLock) RecordEvent(string) {}

func (d *delayedLock) Identity() string { return "delayed" }

func (d *delayedLock) Describe() string { return "delayed" }

// TestTrackingLockRecordsRequestStart pins that a successful Lease write
// counts from when the request was issued, not when its response was
// handled. A renewal whose response straddles a process pause must not read
// as fresh at the resume, or a Lease another replica took in the meantime
// would pass the write gate.
func TestTrackingLockRecordsRequestStart(t *testing.T) {
	const bound = 50 * time.Millisecond
	lock := &trackingLock{Interface: &delayedLock{updateDelay: 2 * bound}}
	if err := lock.Update(t.Context(), resourcelock.LeaderElectionRecord{}); err != nil {
		t.Fatal(err)
	}
	gateErr := (&staleGate{lock: lock, bound: bound}).CanWrite()
	if !errors.Is(gateErr, reconciler.ErrLeadershipStale) {
		t.Fatalf("CanWrite after a slow successful renewal = %v, want ErrLeadershipStale", gateErr)
	}
}

// TestWriteContextCappedAtFreshnessWindow pins the cap anchoring: the write
// deadline lands at the last successful renewal plus the bound, and
// contexts without a gate keep their full timeout.
func TestWriteContextCappedAtFreshnessWindow(t *testing.T) {
	lock := &trackingLock{}
	lock.markSuccess(time.Now())
	gate := &staleGate{lock: lock, bound: 200 * time.Millisecond}
	ctx := reconciler.WithLeaderGate(t.Context(), gate)

	capped, cancel := reconciler.WriteContext(ctx, 30*time.Second)
	defer cancel()
	deadline, ok := capped.Deadline()
	if !ok {
		t.Fatal("capped write context has no deadline")
	}
	if remaining := time.Until(deadline); remaining <= 0 || remaining > 200*time.Millisecond {
		t.Fatalf("write window = %v, want it capped at the 200ms freshness bound", remaining)
	}

	plain, plainCancel := reconciler.WriteContext(t.Context(), time.Hour)
	defer plainCancel()
	plainDeadline, ok := plain.Deadline()
	if !ok || time.Until(plainDeadline) < 59*time.Minute {
		t.Fatal("ungated write context lost its full timeout")
	}
}

// TestWriteContextAbortsRequestAtFreshnessBound pins that a write issued
// inside the freshness window cannot outlive it: a request held by the
// server is aborted at the bound and classified as stale leadership, not
// recorded as a per-desire failure. This covers the pause between the gate
// check and the client call: the deadline is anchored to the renewal, not
// to when the request happens to be issued.
func TestWriteContextAbortsRequestAtFreshnessBound(t *testing.T) {
	const partition = "capped-write"
	for _, typ := range []desire.DesireType{desire.TypeApply, desire.TypeDelete} {
		t.Run(string(typ), func(t *testing.T) {
			requestAborted := make(chan struct{})
			var once sync.Once
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				// Drain the body so the server surfaces the client's abort
				// on req.Context(); the handler must not park with an
				// unread body (same pattern as writes_test.go).
				_, _ = io.Copy(io.Discard, req.Body)
				if req.Method == http.MethodGet {
					w.Header().Set("Content-Type", "application/json")
					_, _ = fmt.Fprint(w, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"target","namespace":"default"}}`)
					return
				}
				// Hold the write until the client aborts it.
				<-req.Context().Done()
				once.Do(func() { close(requestAborted) })
			}))
			t.Cleanup(server.Close)
			dyn, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Version: "v1"}})
			mapper.Add(schema.GroupVersionKind{Version: "v1", Kind: configMapKind}, meta.RESTScopeNamespace)

			store := memory.New()
			applyID := desire.Identity{ManagementCluster: partition, Type: desire.TypeApply,
				Resource: configMapResource, Namespace: testNamespace, Name: testApplyTargetName}
			deleteID := applyID
			deleteID.Type, deleteID.Name = desire.TypeDelete, testDeleteTarget
			content := json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap",
				"metadata":{"name":"target","namespace":"default"},"data":{"key":"value"}}`)
			if _, err = store.CreateApplyDesire(t.Context(), desire.ApplyDesire{
				Identity: applyID, Owner: testOwner, Spec: desire.ApplySpec{KubeContent: content},
			}); err != nil {
				t.Fatal(err)
			}
			if _, err = store.CreateDeleteDesire(t.Context(), desire.DeleteDesire{
				Identity: deleteID, Owner: testOwner,
			}); err != nil {
				t.Fatal(err)
			}

			// Fresh now, but the window closes long before the write's own
			// 30s timeout: the server holds the request past the bound.
			gateCtx, gateCancel := context.WithCancel(t.Context())
			t.Cleanup(gateCancel)
			lock := &trackingLock{}
			lock.markSuccess(time.Now())
			ctx := reconciler.WithLeaderGate(gateCtx, &staleGate{
				lock: lock, bound: 150 * time.Millisecond, cancel: gateCancel,
			})

			start := time.Now()
			if typ == desire.TypeApply {
				err = applydesire.New(store, store, dyn, mapper, partition).RunOnce(ctx)
			} else {
				err = deletedesire.New(store, store, dyn, mapper, partition).RunOnce(ctx)
			}
			if !errors.Is(err, reconciler.ErrLeadershipStale) {
				t.Fatalf("RunOnce error = %v, want ErrLeadershipStale", err)
			}
			select {
			case <-requestAborted:
			case <-time.After(time.Second):
				t.Fatal("in-flight write was not aborted at the freshness bound")
			}
			if elapsed := time.Since(start); elapsed > 2*time.Second {
				t.Fatalf("write outlived the freshness bound: %s", elapsed)
			}
		})
	}
}

// TestStartWaitsForLateControllers pins the scheduling race the acquired
// flag could not cover: client-go starts OnStartedLeading on its own
// goroutine, so Run can return after the acquisition but before the
// callback runs. The Lease write recorded the acquisition synchronously in
// the elector goroutine, so Start must still wait for the controllers the
// callback starts, instead of returning and letting the caller close what
// they use.
func TestStartWaitsForLateControllers(t *testing.T) {
	const partition = "late-callback"
	client := fake.NewClientset()
	controllerDone := make(chan struct{})
	r, err := New(client.CoordinationV1(), testNamespace, partition, "replica",
		funcRunnable(func(ctx context.Context) error {
			defer close(controllerDone)
			<-ctx.Done()
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	callbackStarted := make(chan struct{})
	release := make(chan struct{})
	r.onStartedLeadingDelay = func(context.Context) {
		close(callbackStarted)
		<-release
	}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()

	// Wait for the acquisition (the Lease write runs synchronously in the
	// elector goroutine) and for the callback goroutine to park in the
	// delay, then cancel the election: Run returns while the callback has
	// not run a single statement of the controller startup.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := client.CoordinationV1().Leases(testNamespace).Get(
			t.Context(), LeaseName(partition), metav1.GetOptions{},
		); err == nil {
			break
		} else if !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("elector never wrote the Lease")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-callbackStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("OnStartedLeading goroutine never started")
	}
	cancel()

	// Start must not return while the controllers have not even started,
	// let alone stopped.
	select {
	case err := <-done:
		t.Fatalf("Start returned before the late controllers finished: %v", err)
	case <-controllerDone:
		t.Fatal("controllers ran and exited while the callback was parked")
	case <-time.After(300 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start = %v, want nil after caller cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after the late controllers finished")
	}
	select {
	case <-controllerDone:
	default:
		t.Fatal("controllers did not run after the callback was released")
	}
}

// TestStaleLeadershipStopsIdleControllers covers the watcher directly: a
// controller that never writes must still be stopped once the freshness
// bound passes, well before the elector's own renew deadline.
func TestStaleLeadershipStopsIdleControllers(t *testing.T) {
	const partition = "idle-partition"
	client := fake.NewClientset()
	var fail atomic.Bool
	client.PrependReactor("*", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
		if fail.Load() {
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
	// A 3s renew deadline dwarfs the 100ms watcher, and the 6s backdate
	// dwarfs the 4.9s freshness bound, so only the watcher can produce a
	// prompt exit here.
	r.config.LeaseDuration = 5 * time.Second
	r.config.RenewDeadline = 3 * time.Second
	r.config.RetryPeriod = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("idle controller never started")
	}
	fail.Store(true)
	// A renewal already in flight when the failures began can still land
	// and re-mark lastSuccess; let it settle before backdating, so the
	// backdate is the last word on when the last renewal happened.
	time.Sleep(2 * r.config.RetryPeriod)
	r.lock.backdateLastSuccess(6 * time.Second)

	select {
	case err := <-done:
		if !errors.Is(err, ErrLeadershipLost) {
			t.Fatalf("Start = %v, want ErrLeadershipLost", err)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("idle controller was not stopped by the freshness watcher")
	}
}

// backdateLastSuccess moves the recorded success into the past to simulate
// a process that stalled after renewing.
func (l *trackingLock) backdateLastSuccess(age time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastSuccess = time.Now().Add(-age)
}
