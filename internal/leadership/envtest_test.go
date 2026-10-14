//go:build envtest

package leadership

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	coordination "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/applydesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/deletedesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/serial"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/memory"
)

type electionTransport struct {
	http.RoundTripper
	fail   *atomic.Bool
	writes *atomic.Int32
}

func (r electionTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.Contains(req.URL.Path, "/leases") && r.fail.Load() {
		return nil, errors.New("simulated Lease connectivity failure")
	}
	isWrite := req.Method == http.MethodPatch || req.Method == http.MethodDelete
	if strings.Contains(req.URL.Path, "/configmaps") && isWrite {
		r.writes.Add(1)
	}
	return r.RoundTripper.RoundTrip(req)
}

func TestEnvtestThreeReplicasFailover(t *testing.T) {
	environment := &envtest.Environment{}
	cfg, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	t.Run("renewal failure", func(t *testing.T) { testEnvtestFailover(t, cfg, true) })
	t.Run("leader shutdown", func(t *testing.T) { testEnvtestFailover(t, cfg, false) })
}

func testEnvtestFailover(t *testing.T, cfg *rest.Config, failRenewal bool) {
	t.Helper()
	partition := "shutdown"
	if failRenewal {
		partition = "renewal"
	}
	store := memory.New()
	gvr := schema.GroupVersionResource{Version: "v1", Resource: configMapResource}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Version: "v1"}})
	mapper.AddSpecific(schema.GroupVersionKind{Version: "v1", Kind: configMapKind}, gvr,
		schema.GroupVersionResource{Version: "v1", Resource: "configmap"}, meta.RESTScopeNamespace)
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan int, 3)
	stopped := make(chan int, 3)
	results := make(chan error, 3)
	var active, maximum atomic.Int32
	var failures [3]atomic.Bool
	var writes [3]atomic.Int32
	var stopReplica [3]context.CancelFunc
	for i := range 3 {
		clientConfig := rest.CopyConfig(cfg)
		clientConfig.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
			return electionTransport{RoundTripper: rt, fail: &failures[i], writes: &writes[i]}
		}
		leaseClient, err := coordination.NewForConfig(clientConfig)
		if err != nil {
			t.Fatal(err)
		}
		dyn, err := dynamic.NewForConfig(clientConfig)
		if err != nil {
			t.Fatal(err)
		}
		loop := serial.New(50*time.Millisecond,
			applydesire.New(store, store, dyn, mapper, partition),
			deletedesire.New(store, store, dyn, mapper, partition))
		r, err := New(leaseClient, testNamespace, partition, fmt.Sprintf("replica-%d", i),
			funcRunnable(func(ctx context.Context) error {
				n := active.Add(1)
				for old := maximum.Load(); n > old; old = maximum.Load() {
					if maximum.CompareAndSwap(old, n) {
						break
					}
				}
				started <- i
				defer func() { active.Add(-1); stopped <- i }()
				return loop.Start(ctx)
			}))
		if err != nil {
			t.Fatal(err)
		}
		fastElection(r)
		replicaCtx, stop := context.WithCancel(ctx)
		stopReplica[i] = stop
		go func() { results <- r.Start(replicaCtx) }()
	}
	t.Cleanup(func() {
		cancel()
		for range 3 {
			select {
			case err := <-results:
				if err != nil && !errors.Is(err, ErrLeadershipLost) {
					t.Error(err)
				}
			case <-time.After(5 * time.Second):
				t.Error("replica failed to stop")
			}
		}
	})
	leader := waitReplica(t, started)
	select {
	case other := <-started:
		t.Fatalf("two leaders: %d and %d", leader, other)
	case <-time.After(300 * time.Millisecond):
	}
	// Prevent the leader from renewing its Lease without shutting down the
	// cluster client. Election must cancel reconciliation on its own.
	if failRenewal {
		failures[leader].Store(true)
	} else {
		stopReplica[leader]()
	}
	if old := waitReplica(t, stopped); old != leader {
		t.Fatalf("stopped replica %d, want leader %d", old, leader)
	}
	oldWrites := writes[leader].Load()
	// Create pending desires while no replica is active.
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	deleteName := "delete-target-" + partition
	_, err = dyn.Resource(gvr).Namespace(testNamespace).Create(t.Context(), &unstructured.Unstructured{
		Object: map[string]any{"apiVersion": "v1", "kind": configMapKind, "metadata": map[string]any{
			"name": deleteName, "namespace": testNamespace}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	applyID := desire.Identity{ManagementCluster: partition, Type: desire.TypeApply,
		Resource: configMapResource, Namespace: testNamespace, Name: "apply-target"}
	deleteID := applyID
	deleteID.Type, deleteID.Name = desire.TypeDelete, deleteName
	content := json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap",
		"metadata":{"name":"apply-target","namespace":"default"},"data":{"key":"value"}}`)
	if _, err := store.CreateApplyDesire(t.Context(), desire.ApplyDesire{
		Identity: applyID, Owner: testOwner, Spec: desire.ApplySpec{KubeContent: content},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateDeleteDesire(t.Context(), desire.DeleteDesire{
		Identity: deleteID, Owner: testOwner,
	}); err != nil {
		t.Fatal(err)
	}
	successor := waitReplica(t, started)
	if successor == leader {
		t.Fatal("former leader restarted")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		apply, applyErr := store.GetApplyDesire(t.Context(), applyID)
		del, deleteErr := store.GetDeleteDesire(t.Context(), deleteID)
		if applyErr == nil && deleteErr == nil && hasReason(apply.Status, desire.ReasonApplied) &&
			hasReason(del.Status, desire.ReasonDeleted) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("successor did not reconcile pending desires: apply=%+v delete=%+v", apply, del)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if maximum.Load() != 1 {
		t.Fatalf("maximum simultaneous loops = %d", maximum.Load())
	}
	if writes[leader].Load() != oldWrites {
		t.Fatal("former leader issued writes after losing leadership")
	}
	if writes[successor].Load() < 2 {
		t.Fatal("successor did not issue both SSA and DELETE")
	}
}

func waitReplica(t *testing.T, ch <-chan int) int {
	t.Helper()
	select {
	case i := <-ch:
		return i
	case <-time.After(8 * time.Second):
		t.Fatal("timed out waiting for leadership transition")
		return -1
	}
}

func hasReason(status desire.Status, reason string) bool {
	for _, condition := range status.Conditions {
		if condition.Type == desire.TypeSuccessful && condition.Reason == reason {
			return true
		}
	}
	return false
}
