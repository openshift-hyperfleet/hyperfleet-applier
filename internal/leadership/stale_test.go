package leadership

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/applydesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/deletedesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/serial"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/reconciler"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/memory"
)

func TestStaleGateDeniesAndCancels(t *testing.T) {
	lock := &trackingLock{}
	lock.markSuccess()
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
		Resource: configMapResource, Namespace: testNamespace, Name: "target"}
	deleteID := applyID
	deleteID.Type, deleteID.Name = desire.TypeDelete, "delete-target"
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
		Resource: configMapResource, Namespace: testNamespace, Name: "target"}
	deleteID := applyID
	deleteID.Type, deleteID.Name = desire.TypeDelete, "delete-target"
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

func ptr(s string) *string { return &s }
