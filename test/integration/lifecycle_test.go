//go:build envtest

package integration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/applydesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/deletedesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/readdesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/memory"
)

// TestEnvtest_ApplyReadDeleteLifecycle proves the three controllers
// interoperate correctly through the real desire contract and a real
// apiserver
func TestEnvtest_ApplyReadDeleteLifecycle(t *testing.T) {
	const name = "cm-lifecycle"
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(func() {
		err := envDynamicClient.Resource(configMapGVR).Namespace(defaultNamespace).Delete(
			context.Background(), name, metav1.DeleteOptions{},
		)
		if err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("delete ConfigMap %q: %v", name, err)
		}
	})

	store := memory.New()
	applyID := configMapIdentity(desire.TypeApply, name)
	deleteID := configMapIdentity(desire.TypeDelete, name)
	readID := configMapIdentity(desire.TypeRead, name)

	content := newConfigMapContent(t, name, defaultNamespace, map[string]string{"k": "v1"})
	if _, err := store.CreateApplyDesire(ctx, desire.ApplyDesire{
		Identity: applyID, Owner: testOwner, Spec: desire.ApplySpec{KubeContent: content},
	}); err != nil {
		t.Fatalf("CreateApplyDesire: %v", err)
	}
	if _, err := store.CreateReadDesire(ctx, desire.ReadDesire{
		Identity: readID, Owner: testOwner, TargetVersion: testTargetVersion,
	}); err != nil {
		t.Fatalf("CreateReadDesire: %v", err)
	}

	applyR := applydesire.New(store, store, envDynamicClient, envRESTMapper, testManagementCluster, 50*time.Millisecond)
	deleteR := deletedesire.New(store, store, envDynamicClient, envRESTMapper, testManagementCluster, 50*time.Millisecond)
	readC := readdesire.New(
		store, store, envDynamicClient, envRESTMapper, testManagementCluster,
		100*time.Millisecond,
	)
	go func() { _ = applyR.Start(ctx) }()
	go func() { _ = deleteR.Start(ctx) }()
	go func() { _ = readC.Start(ctx) }()

	// 1. applydesire creates the resource for real via SSA.
	waitForApplyReason(t, ctx, store, applyID, desire.ReasonApplied)

	// 2. readdesire, running independently, observes it.
	got := waitForReadReason(t, ctx, store, readID, desire.ReasonSynced)
	if want := `"k":"v1"`; !strings.Contains(string(got.Status.KubeContent), want) {
		t.Errorf("KubeContent = %s, want it to contain %s", got.Status.KubeContent, want)
	}

	// Version is re-read here rather than
	// reused from the initial create: it's shared across the apply/delete/
	// read sub-states for this target, so applyR's own status write for
	// step 1 already bumped it.
	current, err := store.GetApplyDesire(ctx, applyID)
	if err != nil {
		t.Fatalf("GetApplyDesire before update: %v", err)
	}
	updatedContent := newConfigMapContent(t, name, defaultNamespace, map[string]string{"k": "v2"})
	if _, err := store.UpdateApplyDesireSpec(
		ctx, applyID, desire.ApplySpec{KubeContent: updatedContent}, testOwner, current.Version,
	); err != nil {
		t.Fatalf("UpdateApplyDesireSpec: %v", err)
	}
	waitForReasonAndContent(t, ctx, store, readID, `"k":"v2"`)

	// 4. Creating the DeleteDesire must supersede (clear) the ApplyDesire.
	if _, err := store.CreateDeleteDesire(ctx, desire.DeleteDesire{Identity: deleteID, Owner: testOwner}); err != nil {
		t.Fatalf("CreateDeleteDesire: %v", err)
	}
	if _, err := store.GetApplyDesire(ctx, applyID); !errors.Is(err, desire.ErrNotFound) {
		t.Errorf("GetApplyDesire after CreateDeleteDesire error = %v, want ErrNotFound: "+
			"DeleteDesire must supersede the ApplyDesire", err)
	}

	// 5. deletedesire actually removes the resource.
	dd := waitForDeleteReason(t, ctx, store, deleteID, desire.ReasonDeleted)
	if cond := findCondition(dd.Status, desire.TypeSuccessful); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Errorf("DeleteDesire condition = %+v, want Status=True", cond)
	}

	// 6. readdesire, again independently, observes the deletion.
	waitForReadReason(t, ctx, store, readID, desire.ReasonNotFound)

	// 7. A cleared ApplyDesire must not resurrect the resource: it no longer
	// exists in the store, so applyR's continuous reconcile loop can never
	// act on it again.
	if _, getErr := envDynamicClient.Resource(configMapGVR).Namespace(defaultNamespace).Get(
		ctx, name, metav1.GetOptions{},
	); !apierrors.IsNotFound(getErr) {
		t.Errorf("resource exists after deletion (err=%v), want NotFound: "+
			"a cleared ApplyDesire must not resurrect it", getErr)
	}
}

// TestEnvtest_ApplyReadDeleteLifecycle_ClusterScoped is
// TestEnvtest_ApplyReadDeleteLifecycle's cluster-scoped counterpart: the same
// three-controller interop, just proving it also holds when Identity.Namespace
// is empty (a ClusterRole).
func TestEnvtest_ApplyReadDeleteLifecycle_ClusterScoped(t *testing.T) {
	const name = "cr-lifecycle"
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(func() {
		err := envDynamicClient.Resource(clusterRoleGVR).Delete(context.Background(), name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("delete ClusterRole %q: %v", name, err)
		}
	})

	store := memory.New()
	applyID := clusterRoleIdentity(desire.TypeApply, name)
	deleteID := clusterRoleIdentity(desire.TypeDelete, name)
	readID := clusterRoleIdentity(desire.TypeRead, name)

	content := newClusterRoleContent(t, name)
	if _, err := store.CreateApplyDesire(ctx, desire.ApplyDesire{
		Identity: applyID, Owner: testOwner, Spec: desire.ApplySpec{KubeContent: content},
	}); err != nil {
		t.Fatalf("CreateApplyDesire: %v", err)
	}
	if _, err := store.CreateReadDesire(ctx, desire.ReadDesire{
		Identity: readID, Owner: testOwner, TargetVersion: testTargetVersion,
	}); err != nil {
		t.Fatalf("CreateReadDesire: %v", err)
	}

	applyR := applydesire.New(store, store, envDynamicClient, envRESTMapper, testManagementCluster, 50*time.Millisecond)
	deleteR := deletedesire.New(store, store, envDynamicClient, envRESTMapper, testManagementCluster, 50*time.Millisecond)
	readC := readdesire.New(
		store, store, envDynamicClient, envRESTMapper, testManagementCluster,
		100*time.Millisecond,
	)
	go func() { _ = applyR.Start(ctx) }()
	go func() { _ = deleteR.Start(ctx) }()
	go func() { _ = readC.Start(ctx) }()

	// applydesire creates the cluster-scoped resource; readdesire observes it.
	waitForApplyReason(t, ctx, store, applyID, desire.ReasonApplied)
	got := waitForReadReason(t, ctx, store, readID, desire.ReasonSynced)
	if !strings.Contains(string(got.Status.KubeContent), name) {
		t.Errorf("KubeContent = %s, want it to mention %q", got.Status.KubeContent, name)
	}

	// deletedesire removes it; readdesire independently observes the deletion.
	if _, err := store.CreateDeleteDesire(ctx, desire.DeleteDesire{Identity: deleteID, Owner: testOwner}); err != nil {
		t.Fatalf("CreateDeleteDesire: %v", err)
	}
	waitForDeleteReason(t, ctx, store, deleteID, desire.ReasonDeleted)
	waitForReadReason(t, ctx, store, readID, desire.ReasonNotFound)
}
