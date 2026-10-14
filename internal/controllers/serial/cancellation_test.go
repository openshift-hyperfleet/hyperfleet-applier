package serial

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/applydesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/deletedesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/memory"
)

type cancelBeforeApplyStore struct {
	*memory.Store
	cancel context.CancelFunc
}

func (s cancelBeforeApplyStore) GetApplyDesire(ctx context.Context, id desire.Identity) (desire.ApplyDesire, error) {
	d, err := s.Store.GetApplyDesire(ctx, id)
	s.cancel()
	return d, err
}

func TestCanceledLeadershipDoesNotIssueClusterWrites(t *testing.T) {
	for _, typ := range []desire.DesireType{desire.TypeApply, desire.TypeDelete} {
		t.Run(string(typ), func(t *testing.T) {
			store := memory.New()
			id := configMapIdentity(typ, "target")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			dyn := dynamicfake.NewSimpleDynamicClient(scheme, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: id.Name, Namespace: id.Namespace},
			})
			writes := 0
			dyn.PrependReactor("*", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
				if action.GetVerb() == "patch" || action.GetVerb() == "delete" {
					writes++
				}
				if typ == desire.TypeDelete && action.GetVerb() == "get" {
					// Simulate lease loss between the pre-delete GET and DELETE.
					cancel()
				}
				return false, nil, nil
			})
			if typ == desire.TypeApply {
				if _, err := store.CreateApplyDesire(ctx, desire.ApplyDesire{
					Identity: id, Owner: testOwner,
					Spec: desire.ApplySpec{KubeContent: newConfigMapContent(t, id.Name, id.Namespace, nil)},
				}); err != nil {
					t.Fatal(err)
				}
				rec := applydesire.New(cancelBeforeApplyStore{Store: store, cancel: cancel},
					store, dyn, newTestMapper(), testManagementCluster)
				_ = rec.RunOnce(ctx)
			} else {
				if _, err := store.CreateDeleteDesire(ctx, desire.DeleteDesire{Identity: id, Owner: testOwner}); err != nil {
					t.Fatal(err)
				}
				_ = deletedesire.New(store, store, dyn, newTestMapper(), testManagementCluster).RunOnce(ctx)
			}
			if ctx.Err() == nil {
				t.Fatal("test never canceled leadership")
			}
			if writes != 0 {
				t.Fatalf("cluster writes after cancellation = %d", writes)
			}
		})
	}
}
