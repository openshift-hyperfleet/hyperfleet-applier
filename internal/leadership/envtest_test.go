//go:build envtest

package leadership

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
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
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/readdesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/serial"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/memory"
)

type electionTransport struct {
	http.RoundTripper
	fail   *atomic.Bool
	writes *atomic.Int32
	lists  *atomic.Int32
}

func (r electionTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.Contains(req.URL.Path, "/leases") && r.fail.Load() {
		return nil, errors.New("simulated Lease connectivity failure")
	}
	isWrite := req.Method == http.MethodPatch || req.Method == http.MethodDelete
	if r.writes != nil && isWrite && strings.Contains(req.URL.Path, "/configmaps") {
		r.writes.Add(1)
	}
	// Collection GETs are the ReadDesire informer's LIST/WATCH; apply and
	// delete use single-object requests.
	if r.lists != nil && req.Method == http.MethodGet &&
		strings.HasSuffix(req.URL.Path, "/configmaps") {
		r.lists.Add(1)
	}
	return r.RoundTripper.RoundTrip(req)
}

func TestEnvtestThreeReplicasFailover(t *testing.T) {
	cfg := startEnvtest(t)
	t.Run("renewal failure", func(t *testing.T) { testEnvtestFailover(t, cfg, true) })
	t.Run("leader shutdown", func(t *testing.T) { testEnvtestFailover(t, cfg, false) })
}

// startEnvtest boots a private apiserver for the test and stops it on cleanup.
func startEnvtest(t *testing.T) *rest.Config {
	t.Helper()
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
	return cfg
}

// replicaClients builds the Lease and dynamic clients for one replica,
// wrapping its transport so cluster traffic can be attributed to the replica
// and Lease requests can be failed on demand.
func replicaClients(
	t *testing.T, cfg *rest.Config, transport electionTransport,
) (coordination.CoordinationV1Interface, dynamic.Interface) {
	t.Helper()
	clientConfig := rest.CopyConfig(cfg)
	clientConfig.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		transport.RoundTripper = rt
		return transport
	}
	leaseClient, err := coordination.NewForConfig(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	dyn, err := dynamic.NewForConfig(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	return leaseClient, dyn
}

// trackReplicas registers the cleanup that cancels the replicas and drains
// the results of only those that actually started, so a mid-loop setup
// failure still shuts down and waits for the launched goroutines. It returns
// the function to call once per started replica.
func trackReplicas(t *testing.T, cancel context.CancelFunc, results chan error) func() {
	t.Helper()
	var started int
	t.Cleanup(func() {
		cancel()
		for range started {
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
	return func() { started++ }
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
	replicaStarted := trackReplicas(t, cancel, results)
	for i := range 3 {
		leaseClient, dyn := replicaClients(t, cfg,
			electionTransport{fail: &failures[i], writes: &writes[i]})
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
		replicaStarted()
		replicaCtx, stop := context.WithCancel(ctx)
		stopReplica[i] = stop
		go func() { results <- r.Start(replicaCtx) }()
	}
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

// resettableMapper adapts a meta.RESTMapper to the meta.ResettableRESTMapper
// readdesire requires. Reset is a no-op because the registered mappings never
// change in these tests.
type resettableMapper struct {
	meta.RESTMapper
}

func (*resettableMapper) Reset() {}

// TestEnvtestReadDesireRunsOnlyOnLeader uses the production wiring from
// runServe - serial and readdesire behind the Lease - to pin that ReadDesire
// informers run only on the lease holder, including across a failover.
func TestEnvtestReadDesireRunsOnlyOnLeader(t *testing.T) {
	cfg := startEnvtest(t)

	const partition = "read-partition"
	store := memory.New()
	base := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Version: "v1"}})
	base.AddSpecific(schema.GroupVersionKind{Version: "v1", Kind: configMapKind},
		schema.GroupVersionResource{Version: "v1", Resource: configMapResource},
		schema.GroupVersionResource{Version: "v1", Resource: "configmap"},
		meta.RESTScopeNamespace)
	mapper := &resettableMapper{RESTMapper: base}

	readTarget := seedReadDesire(t, cfg, store, partition, "read-target")

	ctx, cancel := context.WithCancel(t.Context())
	results := make(chan error, 3)
	var failures [3]atomic.Bool
	var lists [3]atomic.Int32
	replicaStarted := trackReplicas(t, cancel, results)
	for i := range 3 {
		leaseClient, dyn := replicaClients(t, cfg,
			electionTransport{fail: &failures[i], lists: &lists[i]})
		r, err := New(leaseClient, testNamespace, partition, fmt.Sprintf("replica-%d", i),
			serial.New(time.Hour,
				applydesire.New(store, store, dyn, mapper, partition),
				deletedesire.New(store, store, dyn, mapper, partition)),
			readdesire.New(store, store, dyn, mapper, partition, 50*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		fastElection(r)
		replicaStarted()
		go func() { results <- r.Start(ctx) }()
	}

	leader := waitForLister(t, lists[:])
	waitForReadSync(t, store, readTarget)
	for i := range lists {
		if i != leader && lists[i].Load() != 0 {
			t.Fatalf("replica %d ran ReadDesire before any failover; want only leader %d", i, leader)
		}
	}

	// The leader loses its lease; a different replica must take over.
	failures[leader].Store(true)
	successor := waitForLister(t, lists[:], leader)
	// Attribution guard: the replica that took over the ReadDesire work must
	// hold the Lease, so standby activity in the failover window cannot pass
	// as successor activity.
	if holder := leaseHolder(t, cfg, partition); holder != fmt.Sprintf("replica-%d", successor) {
		t.Fatalf("lease holder = %q, want the listed replica %d", holder, successor)
	}

	// The successor actively serves ReadDesire, not only holds the lease.
	waitForReadSync(t, store, seedReadDesire(t, cfg, store, partition, "read-target-2"))

	for i := range lists {
		if i != leader && i != successor && lists[i].Load() != 0 {
			t.Fatalf("standby replica %d ran ReadDesire", i)
		}
	}
}

// leaseHolder returns the current holder identity of the partition Lease.
func leaseHolder(t *testing.T, cfg *rest.Config, partition string) string {
	t.Helper()
	client, err := coordination.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := client.Leases(testNamespace).Get(t.Context(), LeaseName(partition), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if lease.Spec.HolderIdentity == nil {
		t.Fatalf("lease %s has no holder", LeaseName(partition))
	}
	return *lease.Spec.HolderIdentity
}

// seedReadDesire creates the cluster object and the ReadDesire mirroring it.
func seedReadDesire(
	t *testing.T, cfg *rest.Config, store *memory.Store, partition, name string,
) desire.Identity {
	t.Helper()
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = dyn.Resource(schema.GroupVersionResource{Version: "v1", Resource: configMapResource}).
		Namespace(testNamespace).Create(t.Context(), &unstructured.Unstructured{
		Object: map[string]any{"apiVersion": "v1", "kind": configMapKind, "metadata": map[string]any{
			"name": name, "namespace": testNamespace}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	id := desire.Identity{ManagementCluster: partition, Type: desire.TypeRead,
		Resource: configMapResource, Namespace: testNamespace, Name: name}
	if _, err := store.CreateReadDesire(t.Context(), desire.ReadDesire{
		Identity: id, Owner: testOwner, TargetVersion: "v1",
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

// waitForLister returns the first replica whose ReadDesire informer issued a
// collection request, skipping the excluded replica numbers.
func waitForLister(t *testing.T, lists []atomic.Int32, exclude ...int) int {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		for i := range lists {
			if lists[i].Load() == 0 || slices.Contains(exclude, i) {
				continue
			}
			return i
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for a replica to run ReadDesire")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitForReadSync(t *testing.T, store *memory.Store, id desire.Identity) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := store.GetReadDesire(t.Context(), id)
		if err == nil && hasReason(got.Status.Status, desire.ReasonSynced) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ReadDesire %s did not reach %s; last status: %+v", id.Name, desire.ReasonSynced, got.Status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
