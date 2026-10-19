# internal/controller/applydesire

`ApplyReconciler` reconciles ApplyDesires (see `pkg/desire/AGENTS.md`) against the local kube-apiserver via server-side apply.

`RunOnce` is one pass over every ApplyDesire. It implements `reconciler.Reconcilable` and does not own a poll loop.
`serial.Runnable` calls it, then `DeleteReconciler.RunOnce`, on one shared cadence.
Ordinary apply failures are recorded in status and are not returned; only non-conflict status-write
failures from individual reconciliations are joined via `errors.Join(...)`. It depends on narrow
unexported interfaces (`specLister`, `statusWriter`), not the full store interfaces.

Context cancellation is treated as control flow, not resource failure: the pass aborts immediately
and does not write failure status during shutdown.

Per desire, `reconcileOne`/`applyToCluster`:
1. Unmarshal `Spec.KubeContent` into an `unstructured.Unstructured`. Invalid JSON, or a manifest
   that cannot be decoded as an object with `kind` (e.g. missing `kind`), or missing
   `apiVersion`/`metadata.name` → `ReasonPreCheckFailed` (no kube-apiserver call attempted).
2. Resolve GVK → GVR via the injected `meta.RESTMapper`. Hosts should supply a
   `restmapper.DeferredDiscoveryRESTMapper` (or equivalent). For `NoMatchError` (e.g. a CRD
   installed after the mapper's discovery cache was already populated), the reconciler resets the
   mapper and retries once before falling back to `ReasonPreCheckFailed` - matching deletedesire's
   and readdesire's identical policy.
3. Reject with `ReasonPreCheckFailed` if the mapped GVR, name, or (for namespaced resources)
   namespace disagree with `d.Identity`. The store validates those fields independently, so a stored
   desire can disagree; applying anyway would mutate a different object than the one status is
   written against. An omitted manifest namespace is not a disagreement; apply uses
   `Identity.Namespace`. **`metadata.name` is required in the manifest** and is not backfilled from
   identity (namespace is backfilled via the client path only).
4. Apply via SSA under the single global field manager (`"hyperfleet-applier"`) with `Force: true`
   - single-writer ownership model, always reclaims contested fields. The SSA target is always
   `d.Identity` (name/namespace), not the raw manifest coordinates.
5. Write the resulting condition back through `StatusStore.UpdateApplyDesireStatus`, using the
   desire's `Version` read at list time. A `ErrVersionConflict` here means spec/status moved since
   `ListApplyDesires` - treated as a benign race, not an error; the next `RunOnce` pass retries.

Every Apply outcome, including `PreCheckFailed` before any Kubernetes call, stamps the listed
desire's `Generation` into `Successful.ObservedGeneration`. `UpdateApplyDesireSpec` retains prior
status, so a condition with an older observed generation remains available as the last known
outcome while the new spec awaits reconciliation. A condition at the current generation reports
the current attempt. `Version` is still used for the status CAS; generation is not a CAS token.

The reconciler reads intent and writes status only. `conditions.Equal` (ignoring
`LastTransitionTime`) suppresses no-op status writes.

## Operational semantics (MVP)

- **Polling only:** `serial.Runnable.Start` provides the finite reconciliation cadence;
  `RunOnce` has no internal workqueue, rate limiter, or per-desire backoff. The hosting binary
  must still configure
  client-side QPS/burst (or equivalent workqueue backoff) so unchanged desires cannot generate
  unbounded SSA traffic.
- **Cost per tick:** every listed desire gets a full SSA round-trip each pass, even when spec and
  cluster are unchanged. Unchanged reconciles suppress the status-store write (and envtest proves a
  true cluster no-op on `resourceVersion`), but not the apiserver call.
- **`ReasonApplied` meaning:** success is recorded when the kube-apiserver **accepts** the SSA
  request. The applier does not GET the live object or diff it against the desired manifest.
- **Apply before status CAS:** SSA runs before `UpdateApplyDesireStatus`. If the status write loses
  a version race, the cluster may briefly hold the list-time manifest while the store has moved on;
  the next pass applies the fresh spec.
- **Cluster-scoped manifests:** `checkApplyTarget` does not reject a non-empty `metadata.namespace`
  on cluster-scoped kinds; the applier forwards it in the SSA body and the kube-apiserver decides
  whether to accept or reject the request.
