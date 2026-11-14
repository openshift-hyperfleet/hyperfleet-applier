// Package conformance is a reusable suite for SpecStore and StatusStore
// backends. RunStatusStoreSuite seeds fixtures through SpecStore.
package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

const (
	resourceConfigMaps   = "configmaps"
	ownerA               = "owner-a"
	kubeContentV1        = `{"v":1}`
	kubeContentV2        = `{"v":2}`
	mutatedStatusMessage = "mutated"
	testTargetVersion    = "v1"
)

func identity(managementCluster string, typ desire.DesireType, name string) desire.Identity {
	return desire.Identity{
		ManagementCluster: managementCluster,
		Type:              typ,
		Group:             "apps",
		Resource:          resourceConfigMaps,
		Namespace:         "default",
		Name:              name,
	}
}

func newApplyDesire(id desire.Identity, owner string, content string) desire.ApplyDesire {
	return desire.ApplyDesire{
		Identity: id,
		Owner:    owner,
		Spec:     desire.ApplySpec{KubeContent: json.RawMessage(content)},
	}
}

func newDeleteDesire(id desire.Identity, owner string) desire.DeleteDesire {
	return desire.DeleteDesire{
		Identity: id,
		Owner:    owner,
	}
}

func newReadDesire(id desire.Identity, owner string) desire.ReadDesire {
	return desire.ReadDesire{
		Identity:      id,
		Owner:         owner,
		TargetVersion: testTargetVersion,
	}
}

func condition(reason string, status metav1.ConditionStatus) metav1.Condition {
	return metav1.Condition{
		Type:               desire.TypeSuccessful,
		Status:             status,
		Reason:             reason,
		Message:            reason,
		LastTransitionTime: metav1.Now(),
	}
}

func seedSpecStore(t *testing.T, s desire.StatusStore) desire.SpecStore {
	t.Helper()
	sp, ok := s.(desire.SpecStore)
	if !ok {
		t.Fatalf("store returned by newStore must also implement desire.SpecStore")
	}
	return sp
}

// RunSpecStoreSuite exercises SpecStore.
func RunSpecStoreSuite(t *testing.T, newStore func(t *testing.T) desire.SpecStore) {
	ctx := context.Background()

	t.Run("ApplyDesire", func(t *testing.T) {
		t.Run("CreateGetRoundTrip", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "cm-1")
			d := newApplyDesire(id, ownerA, `{"kind":"ConfigMap"}`)
			d.Generation = 99 // Generation is store-owned, not caller-supplied.

			created, err := store.CreateApplyDesire(ctx, d)
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}
			if created.Version != 1 {
				t.Errorf("expected Version == 1 after Create, got %d", created.Version)
			}
			if created.Generation != 1 {
				t.Errorf("expected Generation == 1 after Create, got %d", created.Generation)
			}

			got, err := store.GetApplyDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetApplyDesire: %v", err)
			}
			if got.Version != created.Version {
				t.Errorf("expected Get to return same Version as Create, got %d vs %d", got.Version, created.Version)
			}
			if got.Generation != created.Generation {
				t.Errorf("expected Get to return same Generation as Create, got %d vs %d", got.Generation, created.Generation)
			}
		})

		t.Run("CreateExistingReturnsAlreadyExists", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "cm-2")
			d := newApplyDesire(id, ownerA, `{}`)

			if _, err := store.CreateApplyDesire(ctx, d); err != nil {
				t.Fatalf("first CreateApplyDesire: %v", err)
			}
			_, err := store.CreateApplyDesire(ctx, d)
			if !errors.Is(err, desire.ErrAlreadyExists) {
				t.Fatalf("expected ErrAlreadyExists on duplicate create, got %v", err)
			}
		})

		t.Run("CreateDuplicateDifferentOwnerReturnsOwnerConflict", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "owner-first")
			if _, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, `{}`)); err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			_, err := store.CreateApplyDesire(ctx, newApplyDesire(id, "owner-b", `{}`))
			if !errors.Is(err, desire.ErrOwnerConflict) {
				t.Fatalf("expected ErrOwnerConflict for different owner duplicate, got %v", err)
			}
		})

		t.Run("ReturnedSpecIsIsolatedFromCallerMutation", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "clone-spec")
			created, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, kubeContentV1))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			created.Spec.KubeContent[2] = '9'
			got, err := store.GetApplyDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetApplyDesire: %v", err)
			}
			if string(got.Spec.KubeContent) != kubeContentV1 {
				t.Fatalf("expected store to retain original KubeContent, got %q", got.Spec.KubeContent)
			}
		})

		t.Run("InputSpecIsIsolatedFromCallerMutation", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "clone-input")
			d := newApplyDesire(id, ownerA, kubeContentV1)
			if _, err := store.CreateApplyDesire(ctx, d); err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			d.Spec.KubeContent[2] = '9'
			got, err := store.GetApplyDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetApplyDesire: %v", err)
			}
			if string(got.Spec.KubeContent) != kubeContentV1 {
				t.Fatalf("expected store to retain original KubeContent after input mutation, got %q", got.Spec.KubeContent)
			}
		})

		t.Run("UpdateSpecRejectsEmptyKubeContent", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "empty-update")
			created, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, kubeContentV1))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			_, err = store.UpdateApplyDesireSpec(ctx, id, desire.ApplySpec{}, ownerA, created.Version)
			if err == nil {
				t.Fatal("expected UpdateApplyDesireSpec to reject empty KubeContent")
			}
			got, err := store.GetApplyDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetApplyDesire after rejected update: %v", err)
			}
			if got.Generation != created.Generation || got.Version != created.Version {
				t.Errorf("rejected update changed counters: got Generation=%d Version=%d", got.Generation, got.Version)
			}
		})

		t.Run("RecreateAfterDeleteClearsStatus", func(t *testing.T) {
			store := newStore(t)
			statusStore, ok := store.(desire.StatusStore)
			if !ok {
				t.Fatalf("store must also implement desire.StatusStore")
			}
			idApply := identity("cluster-a", desire.TypeApply, "status-clear")
			idRead := identity(idApply.ManagementCluster, desire.TypeRead, idApply.Name)

			created, err := store.CreateApplyDesire(ctx, newApplyDesire(idApply, ownerA, `{}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}
			if _, createReadErr := store.CreateReadDesire(ctx, newReadDesire(idRead, ownerA)); createReadErr != nil {
				t.Fatalf("CreateReadDesire: %v", createReadErr)
			}
			withStatus, err := statusStore.UpdateApplyDesireStatus(
				ctx, idApply,
				desire.Status{Conditions: []metav1.Condition{condition(desire.ReasonApplied, metav1.ConditionTrue)}},
				created.Version, // per-type records: creating the Read did not touch the Apply's version
			)
			if err != nil {
				t.Fatalf("UpdateApplyDesireStatus: %v", err)
			}
			if deleteErr := store.DeleteApplyDesire(ctx, idApply, ownerA, withStatus.Version); deleteErr != nil {
				t.Fatalf("DeleteApplyDesire: %v", deleteErr)
			}

			recreated, err := store.CreateApplyDesire(ctx, newApplyDesire(idApply, ownerA, `{"v":2}`))
			if err != nil {
				t.Fatalf("recreate CreateApplyDesire: %v", err)
			}
			if len(recreated.Status.Conditions) != 0 {
				t.Fatalf("expected recreated Apply to have empty status, got %+v", recreated.Status.Conditions)
			}
			if recreated.Generation != 1 {
				t.Fatalf("expected recreated Apply to start at Generation 1, got %d", recreated.Generation)
			}
		})

		t.Run("GetMissingReturnsNotFound", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "does-not-exist")

			_, err := store.GetApplyDesire(ctx, id)
			if !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected ErrNotFound, got %v", err)
			}
		})

		t.Run("UpdateSpecSuccessIncrementsVersion", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "cm-4")

			created, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, `{"v":1}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			updated, err := store.UpdateApplyDesireSpec(
				ctx, id, desire.ApplySpec{KubeContent: json.RawMessage(kubeContentV2)}, ownerA, created.Version,
			)
			if err != nil {
				t.Fatalf("UpdateApplyDesireSpec: %v", err)
			}
			if updated.Version <= created.Version {
				t.Errorf(
					"expected Version to strictly increase after successful update, got %d -> %d",
					created.Version, updated.Version,
				)
			}
			if updated.Generation != created.Generation+1 {
				t.Errorf("expected Generation %d after spec update, got %d", created.Generation+1, updated.Generation)
			}
		})

		t.Run("OnlyChangedSpecAdvancesGeneration", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "generation-writes")
			const initial = `{"v":1,"meta":{"a":true,"b":[1,2]}}`
			created, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, initial))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}
			statusStore, ok := store.(desire.StatusStore)
			if !ok {
				t.Fatal("store must implement desire.StatusStore")
			}
			observed := condition(desire.ReasonApplied, metav1.ConditionTrue)
			observed.ObservedGeneration = created.Generation
			withStatus, err := statusStore.UpdateApplyDesireStatus(
				ctx, id, desire.Status{Conditions: []metav1.Condition{observed}}, created.Version,
			)
			if err != nil {
				t.Fatalf("UpdateApplyDesireStatus: %v", err)
			}
			before, err := store.GetApplyDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetApplyDesire after status update: %v", err)
			}
			first, err := store.UpdateApplyDesireSpec(
				ctx, id, desire.ApplySpec{KubeContent: json.RawMessage(initial)}, ownerA, withStatus.Version,
			)
			if err != nil {
				t.Fatalf("UpdateApplyDesireSpec with identical content: %v", err)
			}
			const equivalent = ` { "meta": { "b": [1, 2], "a": true }, "v": 1 } `
			second, err := store.UpdateApplyDesireSpec(
				ctx, id, desire.ApplySpec{KubeContent: json.RawMessage(equivalent)}, ownerA, first.Version,
			)
			if err != nil {
				t.Fatalf("UpdateApplyDesireSpec with equivalent JSON: %v", err)
			}
			if first.Generation != created.Generation || second.Generation != created.Generation ||
				first.Version != withStatus.Version+1 || second.Version != first.Version+1 ||
				!reflect.DeepEqual(first.Status, before.Status) || !reflect.DeepEqual(second.Status, before.Status) {
				t.Errorf("equivalent updates altered counters/status: before=%+v first=%+v second=%+v",
					before, first, second)
			}
			if _, statusErr := statusStore.UpdateApplyDesireStatus(
				ctx, id, before.Status, withStatus.Version,
			); !errors.Is(statusErr, desire.ErrVersionConflict) {
				t.Fatalf("stale status update after identical spec write = %v, want ErrVersionConflict", statusErr)
			}
			repeated, err := store.UpdateApplyDesireSpec(
				ctx, id, desire.ApplySpec{KubeContent: json.RawMessage(equivalent)}, ownerA, second.Version,
			)
			if err != nil {
				t.Fatalf("repeated equivalent JSON update: %v", err)
			}
			if repeated.Generation != second.Generation || repeated.Version != second.Version+1 {
				t.Errorf("repeated equivalent update = Generation %d Version %d, want %d and %d",
					repeated.Generation, repeated.Version, second.Generation, second.Version+1)
			}
			const changed = `{"v":2,"meta":{"a":true,"b":[1,2]}}`
			third, err := store.UpdateApplyDesireSpec(
				ctx, id, desire.ApplySpec{KubeContent: json.RawMessage(changed)}, ownerA, repeated.Version,
			)
			if err != nil {
				t.Fatalf("UpdateApplyDesireSpec with changed content: %v", err)
			}
			if len(third.Status.Conditions) != 1 || third.Generation != repeated.Generation+1 ||
				third.Version != repeated.Version+1 ||
				!reflect.DeepEqual(third.Status, before.Status) ||
				third.Status.Conditions[0].ObservedGeneration != created.Generation {
				t.Errorf("changed update did not advance generation while retaining old outcome: got %+v", third)
			}
			if _, updateErr := store.UpdateApplyDesireSpec(
				ctx, id, desire.ApplySpec{KubeContent: json.RawMessage(changed)}, ownerA, repeated.Version,
			); !errors.Is(updateErr, desire.ErrVersionConflict) {
				t.Fatalf("stale spec update error = %v, want ErrVersionConflict", updateErr)
			}
			if _, updateErr := store.UpdateApplyDesireSpec(
				ctx, id, desire.ApplySpec{KubeContent: json.RawMessage(changed)}, "other-owner", third.Version,
			); !errors.Is(updateErr, desire.ErrOwnerConflict) {
				t.Fatalf("foreign owner identical spec update error = %v, want ErrOwnerConflict", updateErr)
			}
			got, err := store.GetApplyDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetApplyDesire after rejected update: %v", err)
			}
			if got.Generation != third.Generation || got.Version != third.Version ||
				!reflect.DeepEqual(got.Status, third.Status) {
				t.Errorf("rejected update changed counters: got Generation=%d Version=%d", got.Generation, got.Version)
			}
		})

		t.Run("UpdateSpecPreservesStatus", func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				status desire.Status
			}{
				{
					name: "PreviousSuccess",
					status: desire.Status{Conditions: []metav1.Condition{{
						Type: desire.TypeSuccessful, Status: metav1.ConditionTrue,
						Reason: desire.ReasonApplied, ObservedGeneration: 1,
						LastTransitionTime: metav1.Now(),
					}}},
				},
				{
					name: "PreviousFailure",
					status: desire.Status{Conditions: []metav1.Condition{{
						Type: desire.TypeSuccessful, Status: metav1.ConditionFalse,
						Reason: desire.ReasonKubeAPIError, Message: "previous API failure",
						ObservedGeneration: 1, LastTransitionTime: metav1.Now(),
					}}},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					store := newStore(t)
					statusStore, ok := store.(desire.StatusStore)
					if !ok {
						t.Fatal("store must also implement desire.StatusStore")
					}
					id := identity("cluster-a", desire.TypeApply, "status-preserved-on-update")
					created, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, kubeContentV1))
					if err != nil {
						t.Fatalf("CreateApplyDesire: %v", err)
					}
					withStatus, err := statusStore.UpdateApplyDesireStatus(ctx, id, tc.status, created.Version)
					if err != nil {
						t.Fatalf("UpdateApplyDesireStatus: %v", err)
					}
					before, err := store.GetApplyDesire(ctx, id)
					if err != nil {
						t.Fatalf("GetApplyDesire before spec update: %v", err)
					}
					updated, err := store.UpdateApplyDesireSpec(
						ctx, id, desire.ApplySpec{KubeContent: json.RawMessage(kubeContentV2)}, ownerA, withStatus.Version,
					)
					if err != nil {
						t.Fatalf("UpdateApplyDesireSpec: %v", err)
					}
					if updated.Generation != created.Generation+1 {
						t.Errorf("Generation = %d, want %d", updated.Generation, created.Generation+1)
					}
					if !reflect.DeepEqual(updated.Status, before.Status) {
						t.Errorf("spec update changed prior status: got %+v, want %+v", updated.Status, before.Status)
					}
					got, err := store.GetApplyDesire(ctx, id)
					if err != nil {
						t.Fatalf("GetApplyDesire: %v", err)
					}
					if !reflect.DeepEqual(got.Status, before.Status) {
						t.Errorf("persisted status changed after spec update: got %+v, want %+v", got.Status, before.Status)
					}
				})
			}
		})

		t.Run("UpdateSpecReturnedValueIsIsolatedFromCallerMutation", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "update-clone-returned")

			created, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, kubeContentV1))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			updated, err := store.UpdateApplyDesireSpec(
				ctx, id, desire.ApplySpec{KubeContent: json.RawMessage(kubeContentV2)}, ownerA, created.Version,
			)
			if err != nil {
				t.Fatalf("UpdateApplyDesireSpec: %v", err)
			}

			updated.Spec.KubeContent[2] = '9'
			got, err := store.GetApplyDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetApplyDesire: %v", err)
			}
			if string(got.Spec.KubeContent) != kubeContentV2 {
				t.Fatalf("expected store to retain updated KubeContent, got %q", got.Spec.KubeContent)
			}
		})

		t.Run("UpdateSpecInputIsIsolatedFromCallerMutation", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "update-clone-input")

			created, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, kubeContentV1))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			spec := desire.ApplySpec{KubeContent: json.RawMessage(kubeContentV2)}
			if _, updateErr := store.UpdateApplyDesireSpec(ctx, id, spec, ownerA, created.Version); updateErr != nil {
				t.Fatalf("UpdateApplyDesireSpec: %v", updateErr)
			}

			spec.KubeContent[2] = '9'
			got, err := store.GetApplyDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetApplyDesire: %v", err)
			}
			if string(got.Spec.KubeContent) != kubeContentV2 {
				t.Fatalf("expected store to retain updated KubeContent after input mutation, got %q", got.Spec.KubeContent)
			}
		})

		t.Run("DifferentOwnerRejected", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "owner-reject")

			created, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, `{"v":1}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			_, err = store.UpdateApplyDesireSpec(
				ctx, id, desire.ApplySpec{KubeContent: json.RawMessage(kubeContentV2)}, "owner-b", created.Version,
			)
			if !errors.Is(err, desire.ErrOwnerConflict) {
				t.Fatalf("expected ErrOwnerConflict for a different owner, got %v", err)
			}
			got, err := store.GetApplyDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetApplyDesire after owner conflict: %v", err)
			}
			if got.Generation != created.Generation || got.Version != created.Version {
				t.Errorf("owner conflict changed counters: got Generation=%d Version=%d", got.Generation, got.Version)
			}
		})

		t.Run("DeleteSuccess", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "cm-6")

			created, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, `{}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}
			if err := store.DeleteApplyDesire(ctx, id, ownerA, created.Version); err != nil {
				t.Fatalf("DeleteApplyDesire: %v", err)
			}
			if _, err := store.GetApplyDesire(ctx, id); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected ErrNotFound after delete, got %v", err)
			}
		})

		t.Run("DeleteStaleVersionRejected", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "cm-stale")

			created, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, kubeContentV1))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}
			err = store.DeleteApplyDesire(ctx, id, ownerA, created.Version+1)
			if !errors.Is(err, desire.ErrVersionConflict) {
				t.Fatalf("expected ErrVersionConflict for stale version, got %v", err)
			}
			got, err := store.GetApplyDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetApplyDesire after stale-version delete: %v", err)
			}
			if got.Version != created.Version {
				t.Errorf("expected Version == %d to survive a stale-version delete, got %d", created.Version, got.Version)
			}
			if string(got.Spec.KubeContent) != kubeContentV1 {
				t.Errorf("stale-version delete changed KubeContent: got %q", got.Spec.KubeContent)
			}
		})

		t.Run("DeleteForeignOwnerRejected", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "cm-owner")

			created, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, kubeContentV1))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}
			err = store.DeleteApplyDesire(ctx, id, "owner-b", created.Version)
			if !errors.Is(err, desire.ErrOwnerConflict) {
				t.Fatalf("expected ErrOwnerConflict for foreign owner, got %v", err)
			}
			got, err := store.GetApplyDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetApplyDesire after foreign-owner delete: %v", err)
			}
			if got.Owner != ownerA {
				t.Errorf("expected Owner == %q to survive a foreign-owner delete, got %q", ownerA, got.Owner)
			}
			if got.Version != created.Version {
				t.Errorf("expected Version == %d to survive a foreign-owner delete, got %d", created.Version, got.Version)
			}
		})
	})

	t.Run("DeleteDesire", func(t *testing.T) {
		t.Run("CreateGetRoundTrip", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeDelete, "del-1")
			d := newDeleteDesire(id, ownerA)
			d.Generation = 99

			created, err := store.CreateDeleteDesire(ctx, d)
			if err != nil {
				t.Fatalf("CreateDeleteDesire: %v", err)
			}
			if created.Version != 1 {
				t.Errorf("expected Version == 1 after Create, got %d", created.Version)
			}
			if created.Generation != 1 {
				t.Errorf("expected Generation == 1 after Create, got %d", created.Generation)
			}

			got, err := store.GetDeleteDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetDeleteDesire: %v", err)
			}
			if got.Identity != id || got.Owner != ownerA || got.Generation != created.Generation {
				t.Errorf("round-tripped desire mismatch: %+v", got)
			}
		})

		t.Run("DeleteSuccess", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeDelete, "del-4")

			created, err := store.CreateDeleteDesire(ctx, newDeleteDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateDeleteDesire: %v", err)
			}
			if err := store.DeleteDeleteDesire(ctx, id, ownerA, created.Version); err != nil {
				t.Fatalf("DeleteDeleteDesire: %v", err)
			}
			if _, err := store.GetDeleteDesire(ctx, id); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected ErrNotFound after delete, got %v", err)
			}
		})

		t.Run("DeleteStaleVersionRejected", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeDelete, "del-stale")

			created, err := store.CreateDeleteDesire(ctx, newDeleteDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateDeleteDesire: %v", err)
			}
			if err := store.DeleteDeleteDesire(ctx, id, ownerA, created.Version+1); !errors.Is(
				err, desire.ErrVersionConflict,
			) {
				t.Fatalf("expected ErrVersionConflict for stale version, got %v", err)
			}
		})

		t.Run("DeleteForeignOwnerRejected", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeDelete, "del-owner")

			created, err := store.CreateDeleteDesire(ctx, newDeleteDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateDeleteDesire: %v", err)
			}
			if err := store.DeleteDeleteDesire(ctx, id, "owner-b", created.Version); !errors.Is(
				err, desire.ErrOwnerConflict,
			) {
				t.Fatalf("expected ErrOwnerConflict for foreign owner, got %v", err)
			}
		})
	})

	t.Run("ReadDesire", func(t *testing.T) {
		t.Run("CreateGetRoundTrip", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeRead, "read-1")
			d := newReadDesire(id, ownerA)
			d.Generation = 99

			created, err := store.CreateReadDesire(ctx, d)
			if err != nil {
				t.Fatalf("CreateReadDesire: %v", err)
			}
			if created.Version != 1 {
				t.Errorf("expected Version == 1 after Create, got %d", created.Version)
			}
			if created.Generation != 1 {
				t.Errorf("expected Generation == 1 after Create, got %d", created.Generation)
			}

			got, err := store.GetReadDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetReadDesire: %v", err)
			}
			if got.Identity != id || got.Owner != ownerA || got.Generation != created.Generation {
				t.Errorf("round-tripped desire mismatch: %+v", got)
			}
		})

		t.Run("DeleteSuccess", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeRead, "read-4")

			created, err := store.CreateReadDesire(ctx, newReadDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateReadDesire: %v", err)
			}
			if err := store.DeleteReadDesire(ctx, id, ownerA, created.Version); err != nil {
				t.Fatalf("DeleteReadDesire: %v", err)
			}
			if _, err := store.GetReadDesire(ctx, id); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected ErrNotFound after delete, got %v", err)
			}
		})

		t.Run("DeleteStaleVersionRejected", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeRead, "read-stale")

			created, err := store.CreateReadDesire(ctx, newReadDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateReadDesire: %v", err)
			}
			if err := store.DeleteReadDesire(ctx, id, ownerA, created.Version+1); !errors.Is(
				err, desire.ErrVersionConflict,
			) {
				t.Fatalf("expected ErrVersionConflict for stale version, got %v", err)
			}
		})

		t.Run("DeleteForeignOwnerRejected", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeRead, "read-owner")

			created, err := store.CreateReadDesire(ctx, newReadDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateReadDesire: %v", err)
			}
			if err := store.DeleteReadDesire(ctx, id, "owner-b", created.Version); !errors.Is(
				err, desire.ErrOwnerConflict,
			) {
				t.Fatalf("expected ErrOwnerConflict for foreign owner, got %v", err)
			}
		})
	})

	t.Run("ManagementClusterListing", func(t *testing.T) {
		t.Run("Apply", func(t *testing.T) {
			store := newStore(t)
			idA := identity("cluster-a", desire.TypeApply, "res-a")
			idB := identity("cluster-b", desire.TypeApply, "res-b")

			if _, err := store.CreateApplyDesire(ctx, newApplyDesire(idA, "owner", `{}`)); err != nil {
				t.Fatalf("CreateApplyDesire(a): %v", err)
			}
			if _, err := store.CreateApplyDesire(ctx, newApplyDesire(idB, "owner", `{}`)); err != nil {
				t.Fatalf("CreateApplyDesire(b): %v", err)
			}

			listA, err := store.ListApplyDesires(ctx, "cluster-a")
			if err != nil {
				t.Fatalf("ListApplyDesires(cluster-a): %v", err)
			}
			if len(listA) != 1 || listA[0].Identity != idA || listA[0].Generation != 1 {
				t.Fatalf("expected only cluster-a's desire, got %+v", listA)
			}

			listB, err := store.ListApplyDesires(ctx, "cluster-b")
			if err != nil {
				t.Fatalf("ListApplyDesires(cluster-b): %v", err)
			}
			if len(listB) != 1 || listB[0].Identity != idB || listB[0].Generation != 1 {
				t.Fatalf("expected only cluster-b's desire, got %+v", listB)
			}
		})

		t.Run("Delete", func(t *testing.T) {
			store := newStore(t)
			idA := identity("cluster-a", desire.TypeDelete, "res-a")
			idB := identity("cluster-b", desire.TypeDelete, "res-b")

			if _, err := store.CreateDeleteDesire(ctx, newDeleteDesire(idA, "owner")); err != nil {
				t.Fatalf("CreateDeleteDesire(a): %v", err)
			}
			if _, err := store.CreateDeleteDesire(ctx, newDeleteDesire(idB, "owner")); err != nil {
				t.Fatalf("CreateDeleteDesire(b): %v", err)
			}

			listA, err := store.ListDeleteDesires(ctx, "cluster-a")
			if err != nil {
				t.Fatalf("ListDeleteDesires(cluster-a): %v", err)
			}
			if len(listA) != 1 || listA[0].Identity != idA || listA[0].Generation != 1 {
				t.Fatalf("expected only cluster-a's desire, got %+v", listA)
			}

			listB, err := store.ListDeleteDesires(ctx, "cluster-b")
			if err != nil {
				t.Fatalf("ListDeleteDesires(cluster-b): %v", err)
			}
			if len(listB) != 1 || listB[0].Identity != idB || listB[0].Generation != 1 {
				t.Fatalf("expected only cluster-b's desire, got %+v", listB)
			}
		})

		t.Run("Read", func(t *testing.T) {
			store := newStore(t)
			idA := identity("cluster-a", desire.TypeRead, "res-a")
			idB := identity("cluster-b", desire.TypeRead, "res-b")

			if _, err := store.CreateReadDesire(ctx, newReadDesire(idA, "owner")); err != nil {
				t.Fatalf("CreateReadDesire(a): %v", err)
			}
			if _, err := store.CreateReadDesire(ctx, newReadDesire(idB, "owner")); err != nil {
				t.Fatalf("CreateReadDesire(b): %v", err)
			}

			listA, err := store.ListReadDesires(ctx, "cluster-a")
			if err != nil {
				t.Fatalf("ListReadDesires(cluster-a): %v", err)
			}
			if len(listA) != 1 || listA[0].Identity != idA || listA[0].Generation != 1 {
				t.Fatalf("expected only cluster-a's desire, got %+v", listA)
			}

			listB, err := store.ListReadDesires(ctx, "cluster-b")
			if err != nil {
				t.Fatalf("ListReadDesires(cluster-b): %v", err)
			}
			if len(listB) != 1 || listB[0].Identity != idB || listB[0].Generation != 1 {
				t.Fatalf("expected only cluster-b's desire, got %+v", listB)
			}
		})
	})

	t.Run("MutualExclusion", func(t *testing.T) {
		t.Run("ApplyThenDeleteSupersedes", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "mutual-1")

			_, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, `{}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			deleteID := identity(id.ManagementCluster, desire.TypeDelete, id.Name)
			_, err = store.CreateDeleteDesire(ctx, newDeleteDesire(deleteID, ownerA))
			if err != nil {
				t.Fatalf("CreateDeleteDesire after Apply: %v", err)
			}

			_, err = store.GetApplyDesire(ctx, id)
			if !errors.Is(err, desire.ErrNotFound) {
				t.Errorf("expected Apply to be superseded by Delete, got err=%v", err)
			}

			_, err = store.GetDeleteDesire(ctx, deleteID)
			if err != nil {
				t.Fatalf("GetDeleteDesire: %v", err)
			}
		})

		t.Run("DeleteThenApplyRejected", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeDelete, "mutual-2")

			_, err := store.CreateDeleteDesire(ctx, newDeleteDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateDeleteDesire: %v", err)
			}

			applyID := identity(id.ManagementCluster, desire.TypeApply, id.Name)
			_, err = store.CreateApplyDesire(ctx, newApplyDesire(applyID, ownerA, `{}`))
			if !errors.Is(err, desire.ErrDeletePending) {
				t.Fatalf("expected ErrDeletePending when Apply follows Delete, got %v", err)
			}
		})

		t.Run("ConfirmedDeleteThenApplyRecreates", func(t *testing.T) {
			store := newStore(t)
			statusStore, ok := store.(desire.StatusStore)
			if !ok {
				t.Fatal("store must also implement StatusStore")
			}
			deleteID := identity("cluster-a", desire.TypeDelete, "mutual-recreate")

			deleted, err := store.CreateDeleteDesire(ctx, newDeleteDesire(deleteID, ownerA))
			if err != nil {
				t.Fatalf("CreateDeleteDesire: %v", err)
			}
			if _, err := statusStore.UpdateDeleteDesireStatus(ctx, deleteID, desire.Status{
				Conditions: []metav1.Condition{condition(desire.ReasonDeleted, metav1.ConditionTrue)},
			}, deleted.Version); err != nil {
				t.Fatalf("UpdateDeleteDesireStatus: %v", err)
			}

			applyID := identity(deleteID.ManagementCluster, desire.TypeApply, deleteID.Name)
			if _, err := store.CreateApplyDesire(ctx, newApplyDesire(applyID, ownerA, `{}`)); err != nil {
				t.Fatalf("CreateApplyDesire after confirmed Delete: %v", err)
			}
			if _, err := store.GetDeleteDesire(ctx, deleteID); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected confirmed DeleteDesire to be retired, got %v", err)
			}
		})

		t.Run("ReadCoexistsWithApply", func(t *testing.T) {
			store := newStore(t)
			idApply := identity("cluster-a", desire.TypeApply, "mutual-4")
			idRead := identity(idApply.ManagementCluster, desire.TypeRead, idApply.Name)

			appliedD, err := store.CreateApplyDesire(ctx, newApplyDesire(idApply, ownerA, `{}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			readD, err := store.CreateReadDesire(ctx, newReadDesire(idRead, ownerA))
			if err != nil {
				t.Fatalf("CreateReadDesire alongside Apply: %v", err)
			}
			// Read has its own Version and does not change Apply's.
			if readD.Version != 1 {
				t.Errorf("expected new Read to start at Version 1, got %d", readD.Version)
			}

			got, err := store.GetApplyDesire(ctx, idApply)
			if err != nil {
				t.Fatalf("GetApplyDesire: %v", err)
			}
			if got.Version != appliedD.Version {
				t.Errorf("expected Apply version unchanged at %d, got %d", appliedD.Version, got.Version)
			}

			gotRead, err := store.GetReadDesire(ctx, idRead)
			if err != nil {
				t.Fatalf("GetReadDesire: %v", err)
			}
			if gotRead.Version != readD.Version {
				t.Errorf("expected Read to coexist, got Version %d vs %d", gotRead.Version, readD.Version)
			}
		})

		t.Run("ReadCoexistsWithDelete", func(t *testing.T) {
			store := newStore(t)
			idDelete := identity("cluster-a", desire.TypeDelete, "mutual-5")
			idRead := identity(idDelete.ManagementCluster, desire.TypeRead, idDelete.Name)

			deletedD, err := store.CreateDeleteDesire(ctx, newDeleteDesire(idDelete, ownerA))
			if err != nil {
				t.Fatalf("CreateDeleteDesire: %v", err)
			}

			readD, err := store.CreateReadDesire(ctx, newReadDesire(idRead, ownerA))
			if err != nil {
				t.Fatalf("CreateReadDesire alongside Delete: %v", err)
			}
			// Read has its own Version and does not change Delete's.
			if readD.Version != 1 {
				t.Errorf("expected new Read to start at Version 1, got %d", readD.Version)
			}

			got, err := store.GetDeleteDesire(ctx, idDelete)
			if err != nil {
				t.Fatalf("GetDeleteDesire: %v", err)
			}
			if got.Version != deletedD.Version {
				t.Errorf("expected Delete version unchanged at %d, got %d", deletedD.Version, got.Version)
			}

			gotRead, err := store.GetReadDesire(ctx, idRead)
			if err != nil {
				t.Fatalf("GetReadDesire: %v", err)
			}
			if gotRead.Version != readD.Version {
				t.Errorf("expected Read to coexist, got Version %d vs %d", gotRead.Version, readD.Version)
			}
		})
	})

	t.Run("CrossTypeOwnership", func(t *testing.T) {
		// All desire types for a target share one owner.
		t.Run("ReadUnderApplyForeignOwnerRejected", func(t *testing.T) {
			store := newStore(t)
			idApply := identity("cluster-a", desire.TypeApply, "cross-owner-1")
			idRead := identity(idApply.ManagementCluster, desire.TypeRead, idApply.Name)

			if _, err := store.CreateApplyDesire(ctx, newApplyDesire(idApply, ownerA, `{}`)); err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}
			if _, err := store.CreateReadDesire(ctx, newReadDesire(idRead, "owner-b")); !errors.Is(
				err, desire.ErrOwnerConflict,
			) {
				t.Fatalf("expected ErrOwnerConflict for foreign owner Read, got %v", err)
			}
		})

		t.Run("DeleteUnderApplyForeignOwnerRejected", func(t *testing.T) {
			store := newStore(t)
			idApply := identity("cluster-a", desire.TypeApply, "cross-owner-2")
			idDelete := identity(idApply.ManagementCluster, desire.TypeDelete, idApply.Name)

			if _, err := store.CreateApplyDesire(ctx, newApplyDesire(idApply, ownerA, `{}`)); err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}
			if _, err := store.CreateDeleteDesire(ctx, newDeleteDesire(idDelete, "owner-b")); !errors.Is(
				err, desire.ErrOwnerConflict,
			) {
				t.Fatalf("expected ErrOwnerConflict for foreign owner Delete, got %v", err)
			}
		})

		t.Run("ApplyUnderReadForeignOwnerRejected", func(t *testing.T) {
			store := newStore(t)
			idRead := identity("cluster-a", desire.TypeRead, "cross-owner-rev-1")
			idApply := identity(idRead.ManagementCluster, desire.TypeApply, idRead.Name)

			if _, err := store.CreateReadDesire(ctx, newReadDesire(idRead, ownerA)); err != nil {
				t.Fatalf("CreateReadDesire: %v", err)
			}
			if _, err := store.CreateApplyDesire(ctx, newApplyDesire(idApply, "owner-b", `{}`)); !errors.Is(
				err, desire.ErrOwnerConflict,
			) {
				t.Fatalf("expected ErrOwnerConflict for foreign owner Apply under Read, got %v", err)
			}
		})

		t.Run("ApplyUnderDeleteForeignOwnerRejected", func(t *testing.T) {
			store := newStore(t)
			idDelete := identity("cluster-a", desire.TypeDelete, "cross-owner-rev-2")
			idApply := identity(idDelete.ManagementCluster, desire.TypeApply, idDelete.Name)

			if _, err := store.CreateDeleteDesire(ctx, newDeleteDesire(idDelete, ownerA)); err != nil {
				t.Fatalf("CreateDeleteDesire: %v", err)
			}
			if _, err := store.CreateApplyDesire(ctx, newApplyDesire(idApply, "owner-b", `{}`)); !errors.Is(
				err, desire.ErrOwnerConflict,
			) {
				t.Fatalf("expected ErrOwnerConflict for foreign owner Apply under Delete, got %v", err)
			}
		})

		t.Run("MatchingOwnerAcrossTypesAllowed", func(t *testing.T) {
			store := newStore(t)
			idRead := identity("cluster-a", desire.TypeRead, "cross-owner-3")
			idApply := identity(idRead.ManagementCluster, desire.TypeApply, idRead.Name)

			if _, err := store.CreateReadDesire(ctx, newReadDesire(idRead, ownerA)); err != nil {
				t.Fatalf("CreateReadDesire: %v", err)
			}
			if _, err := store.CreateApplyDesire(ctx, newApplyDesire(idApply, ownerA, `{}`)); err != nil {
				t.Fatalf("expected same-owner Apply alongside Read to succeed, got %v", err)
			}
		})

		t.Run("MismatchedTypeAccessorsDoNotTouchApply", func(t *testing.T) {
			store := newStore(t)
			statusStore, ok := store.(desire.StatusStore)
			if !ok {
				t.Fatalf("store must also implement desire.StatusStore")
			}
			idApply := identity("cluster-a", desire.TypeApply, "type-guard")

			created, err := store.CreateApplyDesire(ctx, newApplyDesire(idApply, ownerA, `{}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			if _, err = store.GetDeleteDesire(ctx, idApply); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("GetDeleteDesire(apply identity): want ErrNotFound, got %v", err)
			}
			if err = store.DeleteDeleteDesire(ctx, idApply, ownerA, created.Version); !errors.Is(
				err, desire.ErrNotFound,
			) {
				t.Fatalf("DeleteDeleteDesire(apply identity): want ErrNotFound, got %v", err)
			}
			if _, err = store.GetReadDesire(ctx, idApply); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("GetReadDesire(apply identity): want ErrNotFound, got %v", err)
			}
			if err = store.DeleteReadDesire(ctx, idApply, ownerA, created.Version); !errors.Is(
				err, desire.ErrNotFound,
			) {
				t.Fatalf("DeleteReadDesire(apply identity): want ErrNotFound, got %v", err)
			}

			if _, err = statusStore.UpdateDeleteDesireStatus(
				ctx, idApply, desire.Status{}, created.Version,
			); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("UpdateDeleteDesireStatus(apply identity): want ErrNotFound, got %v", err)
			}
			if _, err = statusStore.UpdateReadDesireStatus(
				ctx, idApply, desire.ReadStatus{},
			); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("UpdateReadDesireStatus(apply identity): want ErrNotFound, got %v", err)
			}

			got, err := store.GetApplyDesire(ctx, idApply)
			if err != nil {
				t.Fatalf("GetApplyDesire after mismatched accessors: %v", err)
			}
			if got.Version != created.Version {
				t.Fatalf("expected Apply version %d unchanged, got %d", created.Version, got.Version)
			}
		})
	})

	t.Run("DeleteByPrefix", func(t *testing.T) {
		t.Run("RemovesMatchingApplyOnly", func(t *testing.T) {
			store := newStore(t)
			keep := identity("cluster-a", desire.TypeApply, "keep")
			drop := identity("cluster-a", desire.TypeApply, "drop")
			otherCluster := identity("cluster-b", desire.TypeApply, "drop")

			for _, id := range []desire.Identity{keep, drop, otherCluster} {
				if _, err := store.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, `{}`)); err != nil {
					t.Fatalf("CreateApplyDesire(%s): %v", id.Name, err)
				}
			}

			err := store.DeleteByPrefix(ctx, "cluster-a", desire.PrefixSelector{
				Type: desire.TypeApply,
				Name: "drop",
			})
			if err != nil {
				t.Fatalf("DeleteByPrefix: %v", err)
			}

			if _, err := store.GetApplyDesire(ctx, drop); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected drop to be deleted, got %v", err)
			}
			if _, err := store.GetApplyDesire(ctx, keep); err != nil {
				t.Fatalf("expected keep to remain, got %v", err)
			}
			if _, err := store.GetApplyDesire(ctx, otherCluster); err != nil {
				t.Fatalf("expected other cluster desire to remain, got %v", err)
			}
		})

		t.Run("ClearsOneSubStateLeavesOthers", func(t *testing.T) {
			store := newStore(t)
			idApply := identity("cluster-a", desire.TypeApply, "shared")
			idRead := identity(idApply.ManagementCluster, desire.TypeRead, idApply.Name)

			if _, err := store.CreateApplyDesire(ctx, newApplyDesire(idApply, ownerA, `{}`)); err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}
			if _, err := store.CreateReadDesire(ctx, newReadDesire(idRead, ownerA)); err != nil {
				t.Fatalf("CreateReadDesire: %v", err)
			}

			err := store.DeleteByPrefix(ctx, "cluster-a", desire.PrefixSelector{
				Type: desire.TypeApply,
				Name: "shared",
			})
			if err != nil {
				t.Fatalf("DeleteByPrefix: %v", err)
			}

			if _, err := store.GetApplyDesire(ctx, idApply); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected Apply cleared, got %v", err)
			}
			if _, err := store.GetReadDesire(ctx, idRead); err != nil {
				t.Fatalf("expected Read to remain, got %v", err)
			}
		})

		t.Run("MatchesResourcePrefix", func(t *testing.T) {
			store := newStore(t)
			cm := identity("cluster-a", desire.TypeDelete, "obj-1")
			secret := identity("cluster-a", desire.TypeDelete, "obj-2")
			secret.Resource = "secrets"

			if _, err := store.CreateDeleteDesire(ctx, newDeleteDesire(cm, ownerA)); err != nil {
				t.Fatalf("CreateDeleteDesire(cm): %v", err)
			}
			if _, err := store.CreateDeleteDesire(ctx, newDeleteDesire(secret, ownerA)); err != nil {
				t.Fatalf("CreateDeleteDesire(secret): %v", err)
			}

			err := store.DeleteByPrefix(ctx, "cluster-a", desire.PrefixSelector{
				Type:     desire.TypeDelete,
				Resource: "config",
			})
			if err != nil {
				t.Fatalf("DeleteByPrefix: %v", err)
			}

			if _, err := store.GetDeleteDesire(ctx, cm); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected configmaps delete desire removed, got %v", err)
			}
			if _, err := store.GetDeleteDesire(ctx, secret); err != nil {
				t.Fatalf("expected secrets delete desire to remain, got %v", err)
			}
		})

		t.Run("PartialClearLeavesSiblingVersionUntouched", func(t *testing.T) {
			store := newStore(t)
			idApply := identity("cluster-a", desire.TypeApply, "ver-bump")
			idRead := identity(idApply.ManagementCluster, desire.TypeRead, idApply.Name)

			applied, err := store.CreateApplyDesire(ctx, newApplyDesire(idApply, ownerA, `{}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}
			if _, createErr := store.CreateReadDesire(ctx, newReadDesire(idRead, ownerA)); createErr != nil {
				t.Fatalf("CreateReadDesire: %v", createErr)
			}

			if delErr := store.DeleteByPrefix(ctx, "cluster-a", desire.PrefixSelector{
				Type: desire.TypeRead,
				Name: idRead.Name,
			}); delErr != nil {
				t.Fatalf("DeleteByPrefix: %v", delErr)
			}

			// Clearing Read must not change Apply's Version.
			got, getErr := store.GetApplyDesire(ctx, idApply)
			if getErr != nil {
				t.Fatalf("GetApplyDesire after prefix delete: %v", getErr)
			}
			if got.Version != applied.Version {
				t.Fatalf("expected Apply Version unchanged at %d after clearing Read, got %d", applied.Version, got.Version)
			}

			if _, updateErr := store.UpdateApplyDesireSpec(
				ctx, idApply, desire.ApplySpec{KubeContent: json.RawMessage(`{"v":2}`)}, ownerA, applied.Version,
			); updateErr != nil {
				t.Fatalf("expected update with unchanged version %d to succeed, got %v", applied.Version, updateErr)
			}
		})
	})
}

// RunStatusStoreSuite exercises StatusStore.
func RunStatusStoreSuite(t *testing.T, newStore func(t *testing.T) desire.StatusStore) {
	ctx := context.Background()

	t.Run("ApplyDesireStatus", func(t *testing.T) {
		t.Run("GetMissingReturnsNotFound", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeApply, "missing")

			if _, err := store.GetApplyDesire(ctx, id); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected ErrNotFound, got %v", err)
			}
		})

		t.Run("UpdateStatusSuccessIncrementsVersion", func(t *testing.T) {
			store := newStore(t)
			spec := seedSpecStore(t, store)
			id := identity("cluster-a", desire.TypeApply, "cm-2")

			created, err := spec.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, `{}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			updated, err := store.UpdateApplyDesireStatus(
				ctx, id,
				desire.Status{Conditions: []metav1.Condition{condition(desire.ReasonApplied, metav1.ConditionTrue)}},
				created.Version,
			)
			if err != nil {
				t.Fatalf("UpdateApplyDesireStatus: %v", err)
			}
			if updated.Version <= created.Version {
				t.Errorf(
					"expected Version to strictly increase after successful status update, got %d -> %d",
					created.Version, updated.Version,
				)
			}
			if updated.Generation != created.Generation {
				t.Errorf("status write changed Generation from %d to %d", created.Generation, updated.Generation)
			}
		})

		t.Run("StaleStatusWriteAfterSpecUpdateIsRejected", func(t *testing.T) {
			store := newStore(t)
			spec := seedSpecStore(t, store)
			id := identity("cluster-a", desire.TypeApply, "stale-status-after-spec")
			created, err := spec.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, kubeContentV1))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}
			oldCondition := condition(desire.ReasonApplied, metav1.ConditionTrue)
			oldCondition.ObservedGeneration = created.Generation
			withStatus, err := store.UpdateApplyDesireStatus(
				ctx, id, desire.Status{Conditions: []metav1.Condition{oldCondition}}, created.Version,
			)
			if err != nil {
				t.Fatalf("UpdateApplyDesireStatus: %v", err)
			}
			before, err := store.GetApplyDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetApplyDesire before spec update: %v", err)
			}
			updated, err := spec.UpdateApplyDesireSpec(
				ctx, id, desire.ApplySpec{KubeContent: json.RawMessage(kubeContentV2)}, ownerA, withStatus.Version,
			)
			if err != nil {
				t.Fatalf("UpdateApplyDesireSpec: %v", err)
			}
			if updated.Generation != withStatus.Generation+1 || updated.Version != withStatus.Version+1 ||
				!reflect.DeepEqual(updated.Status, before.Status) {
				t.Fatalf("after spec update = %+v, want advanced counters and preserved status from %+v", updated, before)
			}

			staleCondition := condition(desire.ReasonKubeAPIError, metav1.ConditionFalse)
			staleCondition.ObservedGeneration = withStatus.Generation
			_, err = store.UpdateApplyDesireStatus(
				ctx, id, desire.Status{Conditions: []metav1.Condition{staleCondition}}, withStatus.Version,
			)
			if !errors.Is(err, desire.ErrVersionConflict) {
				t.Fatalf("stale status update error = %v, want ErrVersionConflict", err)
			}
			got, err := store.GetApplyDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetApplyDesire after stale status update: %v", err)
			}
			if got.Generation != updated.Generation || got.Version != updated.Version ||
				string(got.Spec.KubeContent) != kubeContentV2 || !reflect.DeepEqual(got.Status, updated.Status) {
				t.Errorf("stale status write changed record: got %+v, want %+v", got, updated)
			}
		})

		t.Run("UpdateStatusReturnedValueIsIsolatedFromCallerMutation", func(t *testing.T) {
			store := newStore(t)
			spec := seedSpecStore(t, store)
			id := identity("cluster-a", desire.TypeApply, "status-clone-returned")

			created, err := spec.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, `{}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			updated, err := store.UpdateApplyDesireStatus(
				ctx, id,
				desire.Status{Conditions: []metav1.Condition{condition(desire.ReasonApplied, metav1.ConditionTrue)}},
				created.Version,
			)
			if err != nil {
				t.Fatalf("UpdateApplyDesireStatus: %v", err)
			}

			updated.Status.Conditions[0].Message = mutatedStatusMessage
			got, err := store.GetApplyDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetApplyDesire: %v", err)
			}
			if got.Status.Conditions[0].Message != desire.ReasonApplied {
				t.Fatalf("expected store to retain original status message, got %q", got.Status.Conditions[0].Message)
			}
		})

		t.Run("UpdateStatusInputIsIsolatedFromCallerMutation", func(t *testing.T) {
			store := newStore(t)
			spec := seedSpecStore(t, store)
			id := identity("cluster-a", desire.TypeApply, "status-clone-input")

			created, err := spec.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, `{}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			status := desire.Status{Conditions: []metav1.Condition{condition(desire.ReasonApplied, metav1.ConditionTrue)}}
			if _, updateErr := store.UpdateApplyDesireStatus(ctx, id, status, created.Version); updateErr != nil {
				t.Fatalf("UpdateApplyDesireStatus: %v", updateErr)
			}

			status.Conditions[0].Message = mutatedStatusMessage
			got, err := store.GetApplyDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetApplyDesire: %v", err)
			}
			if got.Status.Conditions[0].Message != desire.ReasonApplied {
				t.Fatalf("expected store to retain original status after input mutation, got %q", got.Status.Conditions[0].Message)
			}
		})

		t.Run("StatusWriteDoesNotChangeSpec", func(t *testing.T) {
			store := newStore(t)
			spec := seedSpecStore(t, store)
			id := identity("cluster-a", desire.TypeApply, "cm-3")

			created, err := spec.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, `{"v":1}`))
			if err != nil {
				t.Fatalf("CreateApplyDesire: %v", err)
			}

			afterStatus, err := store.UpdateApplyDesireStatus(
				ctx, id,
				desire.Status{Conditions: []metav1.Condition{condition(desire.ReasonApplied, metav1.ConditionTrue)}},
				created.Version,
			)
			if err != nil {
				t.Fatalf("UpdateApplyDesireStatus: %v", err)
			}
			if string(afterStatus.Spec.KubeContent) != `{"v":1}` {
				t.Errorf("UpdateApplyDesireStatus must not change Spec, got %q", afterStatus.Spec.KubeContent)
			}
			if afterStatus.Generation != created.Generation {
				t.Errorf("UpdateApplyDesireStatus changed Generation from %d to %d", created.Generation, afterStatus.Generation)
			}
		})
	})

	t.Run("DeleteDesireStatus", func(t *testing.T) {
		t.Run("GetMissingReturnsNotFound", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeDelete, "missing")

			if _, err := store.GetDeleteDesire(ctx, id); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected ErrNotFound, got %v", err)
			}
		})

		t.Run("UpdateStatusSuccessIncrementsVersion", func(t *testing.T) {
			store := newStore(t)
			spec := seedSpecStore(t, store)
			id := identity("cluster-a", desire.TypeDelete, "del-2")

			created, err := spec.CreateDeleteDesire(ctx, newDeleteDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateDeleteDesire: %v", err)
			}

			status := desire.Status{Conditions: []metav1.Condition{
				condition(desire.ReasonWaitingForDeletion, metav1.ConditionFalse),
			}}
			updated, err := store.UpdateDeleteDesireStatus(ctx, id, status, created.Version)
			if err != nil {
				t.Fatalf("UpdateDeleteDesireStatus: %v", err)
			}
			if updated.Version <= created.Version {
				t.Errorf(
					"expected Version to strictly increase after successful status update, got %d -> %d",
					created.Version, updated.Version,
				)
			}
			if updated.Generation != created.Generation {
				t.Errorf("delete status write changed Generation from %d to %d", created.Generation, updated.Generation)
			}

			got, err := store.GetDeleteDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetDeleteDesire: %v", err)
			}
			if len(got.Status.Conditions) != 1 || got.Status.Conditions[0].Reason != desire.ReasonWaitingForDeletion {
				t.Errorf("expected persisted delete status, got %+v", got.Status.Conditions)
			}
		})

		t.Run("UpdateStatusReturnedValueIsIsolatedFromCallerMutation", func(t *testing.T) {
			store := newStore(t)
			spec := seedSpecStore(t, store)
			id := identity("cluster-a", desire.TypeDelete, "del-status-clone-returned")

			created, err := spec.CreateDeleteDesire(ctx, newDeleteDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateDeleteDesire: %v", err)
			}

			status := desire.Status{Conditions: []metav1.Condition{
				condition(desire.ReasonWaitingForDeletion, metav1.ConditionFalse),
			}}
			updated, err := store.UpdateDeleteDesireStatus(ctx, id, status, created.Version)
			if err != nil {
				t.Fatalf("UpdateDeleteDesireStatus: %v", err)
			}

			updated.Status.Conditions[0].Message = mutatedStatusMessage
			got, err := store.GetDeleteDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetDeleteDesire: %v", err)
			}
			if got.Status.Conditions[0].Message != desire.ReasonWaitingForDeletion {
				t.Fatalf("expected store to retain original delete status message, got %q", got.Status.Conditions[0].Message)
			}
		})

		t.Run("StaleVersionRejected", func(t *testing.T) {
			store := newStore(t)
			spec := seedSpecStore(t, store)
			id := identity("cluster-a", desire.TypeDelete, "del-3")

			created, err := spec.CreateDeleteDesire(ctx, newDeleteDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateDeleteDesire: %v", err)
			}

			status := desire.Status{Conditions: []metav1.Condition{
				condition(desire.ReasonWaitingForDeletion, metav1.ConditionFalse),
			}}
			if _, updateErr := store.UpdateDeleteDesireStatus(ctx, id, status, created.Version); updateErr != nil {
				t.Fatalf("UpdateDeleteDesireStatus: %v", updateErr)
			}
			_, staleErr := store.UpdateDeleteDesireStatus(ctx, id, status, created.Version)
			if !errors.Is(staleErr, desire.ErrVersionConflict) {
				t.Fatalf("expected ErrVersionConflict on stale version, got %v", staleErr)
			}
		})
	})

	t.Run("ReadDesireStatus", func(t *testing.T) {
		t.Run("GetMissingReturnsNotFound", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeRead, "missing")

			if _, err := store.GetReadDesire(ctx, id); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected ErrNotFound, got %v", err)
			}
		})

		t.Run("UpdateMissingReturnsNotFound", func(t *testing.T) {
			store := newStore(t)
			id := identity("cluster-a", desire.TypeRead, "missing")

			status := desire.ReadStatus{
				Status: desire.Status{Conditions: []metav1.Condition{condition(desire.ReasonSynced, metav1.ConditionTrue)}},
			}
			if _, err := store.UpdateReadDesireStatus(ctx, id, status); !errors.Is(err, desire.ErrNotFound) {
				t.Fatalf("expected ErrNotFound, got %v", err)
			}
		})

		t.Run("UpdateStatusSucceedsWithoutVersionCheck", func(t *testing.T) {
			store := newStore(t)
			spec := seedSpecStore(t, store)
			id := identity("cluster-a", desire.TypeRead, "read-2")

			if _, err := spec.CreateReadDesire(ctx, newReadDesire(id, ownerA)); err != nil {
				t.Fatalf("CreateReadDesire: %v", err)
			}

			status := desire.ReadStatus{
				Status: desire.Status{Conditions: []metav1.Condition{condition(desire.ReasonSynced, metav1.ConditionTrue)}},
			}
			updated, err := store.UpdateReadDesireStatus(ctx, id, status)
			if err != nil {
				t.Fatalf("UpdateReadDesireStatus: %v", err)
			}
			if updated.Generation != 1 {
				t.Errorf("read status write changed Generation to %d, want 1", updated.Generation)
			}
		})

		t.Run("RepeatedWritesNeverFail", func(t *testing.T) {
			store := newStore(t)
			spec := seedSpecStore(t, store)
			id := identity("cluster-a", desire.TypeRead, "read-4")

			if _, err := spec.CreateReadDesire(ctx, newReadDesire(id, ownerA)); err != nil {
				t.Fatalf("CreateReadDesire: %v", err)
			}

			status := desire.ReadStatus{
				Status: desire.Status{Conditions: []metav1.Condition{condition(desire.ReasonSynced, metav1.ConditionTrue)}},
			}
			for i := range 5 {
				if _, err := store.UpdateReadDesireStatus(ctx, id, status); err != nil {
					t.Fatalf("UpdateReadDesireStatus attempt %d: %v", i, err)
				}
			}
		})

		t.Run("DoesNotAdvanceVersionApplyOrDeleteCASDependsOn", func(t *testing.T) {
			readStatus := desire.ReadStatus{
				Status: desire.Status{Conditions: []metav1.Condition{condition(desire.ReasonSynced, metav1.ConditionTrue)}},
			}

			t.Run("Apply", func(t *testing.T) {
				store := newStore(t)
				spec := seedSpecStore(t, store)
				id := identity("cluster-a", desire.TypeApply, "shared-1")
				readID := id
				readID.Type = desire.TypeRead

				createdApply, err := spec.CreateApplyDesire(ctx, newApplyDesire(id, ownerA, kubeContentV1))
				if err != nil {
					t.Fatalf("CreateApplyDesire: %v", err)
				}
				if _, err := spec.CreateReadDesire(ctx, newReadDesire(readID, ownerA)); err != nil {
					t.Fatalf("CreateReadDesire: %v", err)
				}

				// Read status writes must not change Apply's Version.
				if _, err := store.UpdateReadDesireStatus(ctx, readID, readStatus); err != nil {
					t.Fatalf("UpdateReadDesireStatus: %v", err)
				}

				applyStatus := desire.Status{
					Conditions: []metav1.Condition{condition(desire.ReasonApplied, metav1.ConditionTrue)},
				}
				if _, err := store.UpdateApplyDesireStatus(ctx, id, applyStatus, createdApply.Version); err != nil {
					t.Fatalf("UpdateApplyDesireStatus after Read status write failed: %v", err)
				}
			})

			t.Run("Delete", func(t *testing.T) {
				store := newStore(t)
				spec := seedSpecStore(t, store)
				id := identity("cluster-a", desire.TypeDelete, "shared-2")
				readID := id
				readID.Type = desire.TypeRead

				createdDelete, err := spec.CreateDeleteDesire(ctx, newDeleteDesire(id, ownerA))
				if err != nil {
					t.Fatalf("CreateDeleteDesire: %v", err)
				}
				if _, err := spec.CreateReadDesire(ctx, newReadDesire(readID, ownerA)); err != nil {
					t.Fatalf("CreateReadDesire: %v", err)
				}

				// Read status writes must not change Delete's Version.
				if _, err := store.UpdateReadDesireStatus(ctx, readID, readStatus); err != nil {
					t.Fatalf("UpdateReadDesireStatus: %v", err)
				}

				deleteStatus := desire.Status{
					Conditions: []metav1.Condition{condition(desire.ReasonWaitingForDeletion, metav1.ConditionFalse)},
				}
				if _, err := store.UpdateDeleteDesireStatus(ctx, id, deleteStatus, createdDelete.Version); err != nil {
					t.Fatalf("UpdateDeleteDesireStatus after Read status write failed: %v", err)
				}
			})
		})

		t.Run("UpdateStatusReturnedValueIsIsolatedFromCallerMutation", func(t *testing.T) {
			store := newStore(t)
			spec := seedSpecStore(t, store)
			id := identity("cluster-a", desire.TypeRead, "read-status-clone-returned")

			if _, err := spec.CreateReadDesire(ctx, newReadDesire(id, ownerA)); err != nil {
				t.Fatalf("CreateReadDesire: %v", err)
			}

			content := json.RawMessage(`{"kind":"ConfigMap","data":{"k":"v"}}`)
			status := desire.ReadStatus{
				Status:      desire.Status{Conditions: []metav1.Condition{condition(desire.ReasonSynced, metav1.ConditionTrue)}},
				KubeContent: content,
			}
			updated, err := store.UpdateReadDesireStatus(ctx, id, status)
			if err != nil {
				t.Fatalf("UpdateReadDesireStatus: %v", err)
			}

			updated.Status.KubeContent[2] = '9'
			updated.Status.Conditions[0].Message = mutatedStatusMessage
			got, err := store.GetReadDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetReadDesire: %v", err)
			}
			if string(got.Status.KubeContent) != string(content) {
				t.Fatalf("expected store to retain original KubeContent, got %q", got.Status.KubeContent)
			}
			if got.Status.Conditions[0].Message != desire.ReasonSynced {
				t.Fatalf("expected store to retain original status message, got %q", got.Status.Conditions[0].Message)
			}
		})

		t.Run("KubeContentMirror", func(t *testing.T) {
			store := newStore(t)
			spec := seedSpecStore(t, store)
			id := identity("cluster-a", desire.TypeRead, "read-3")

			created, err := spec.CreateReadDesire(ctx, newReadDesire(id, ownerA))
			if err != nil {
				t.Fatalf("CreateReadDesire: %v", err)
			}
			if created.Status.KubeContent != nil {
				t.Errorf("expected KubeContent to be nil/absent before it is ever set, got %q", created.Status.KubeContent)
			}

			content := json.RawMessage(`{"kind":"ConfigMap","data":{"k":"v"}}`)
			readStatus := desire.ReadStatus{
				Status:      desire.Status{Conditions: []metav1.Condition{condition(desire.ReasonSynced, metav1.ConditionTrue)}},
				KubeContent: content,
			}
			updated, err := store.UpdateReadDesireStatus(ctx, id, readStatus)
			if err != nil {
				t.Fatalf("UpdateReadDesireStatus: %v", err)
			}
			if string(updated.Status.KubeContent) != string(content) {
				t.Errorf("expected returned desire to reflect KubeContent, got %q", updated.Status.KubeContent)
			}

			got2, err := store.GetReadDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetReadDesire: %v", err)
			}
			if string(got2.Status.KubeContent) != string(content) {
				t.Errorf(
					"expected GetReadDesire to reflect KubeContent set via UpdateReadDesireStatus, got %q",
					got2.Status.KubeContent,
				)
			}
		})
	})
}
