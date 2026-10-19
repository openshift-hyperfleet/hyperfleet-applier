# internal/controllers/readdesire

`Controller` mirrors live Kubernetes object state (see `pkg/desire/AGENTS.md`) back into
`ReadDesire` status, for one management-cluster partition.

## Shape: one informer per `ReadDesire`, not per resource type

Each `ReadDesire` gets its own Kubernetes informer, scoped via a `metadata.name` field selector
(plus namespace) to exactly the object it targets - not a shared informer per `GroupVersionResource`
watching every object of that type. This means event handlers need no object introspection to know
which desire an event belongs to: the informer's existence and scope *is* the filter, so
`AddFunc`/`UpdateFunc`/`DeleteFunc` just enqueue the `desire.Identity` they were built with.

The workqueue's key type is `desire.Identity` itself (a plain `comparable` struct), not a string
encoding of it - `workqueue.TypedRateLimitingInterface[desire.Identity]`. Every key this package
ever handles has `Type == desire.TypeRead`, since this controller only ever deals with ReadDesires;
there's no separate type-erased resource-key abstraction to project into and back out of - `Identity`
already carries `Namespace`/`Name`/`Group`/`Resource` directly, and doubles as the store call
argument with no reconstruction step.

**Accepted tradeoff:** resource usage (goroutines, apiserver watch connections) scales with the
*number* of `ReadDesire`s, not the number of distinct resource types - heavier than a shared-per-GVR
design at high desire counts. Traded here for precision (handlers never need to filter) and
simplicity (no GVR-level reference counting across multiple desires).

## Poll loop: lifecycle only, never enqueues

`Start` launches the worker pool and calls `pollOnce` immediately, then every `pollInterval`.
Each poll calls `ListReadDesires` and reconciles the running per-desire
informer set against it (`InformerManager.Reconcile`): start an informer for a newly-listed
`ReadDesire`, stop one whose `ReadDesire` is gone. **`pollOnce`/`Reconcile` never call `queue.Add`
themselves.** Once a per-desire informer exists, its own initial `List` (fires `AddFunc` if the
object currently exists) and its 60s `resyncPeriod` (fires `UpdateFunc` on that same cadence
thereafter, replaying the cached object) are what keep that desire's key flowing into the queue for
as long as the informer runs - "let resyncing and relisting handle it."

The one exception is `InformerManager.start` itself: after a newly-started informer's
`cache.WaitForCacheSync` succeeds, it enqueues the key once, unconditionally - not as a substitute
for `AddFunc`/`UpdateFunc`/`DeleteFunc` (those still do the ongoing work), but because `AddFunc` only
fires if the target *exists*. Kubernetes has no event for "object still absent" at all, so without
this, a `ReadDesire` whose target doesn't exist yet would get an informer immediately but never get
a single sync out of it - not even a `NotFound` write - until/unless the object is later created;
its status would just sit at whatever `CreateReadDesire` left it (no `Successful` condition at all).
The post-sync enqueue guarantees exactly one sync against a cache that's confirmed populated (or
confirmed empty), so `observe` gets an authoritative initial read either way. If `AddFunc` also fired
for that same initial list (the object does exist), this is a harmless no-op: the workqueue dedups,
so it's at most one redundant extra sync, not a duplicate status write (`readStatusEqual` suppresses
that regardless).

**Still an accepted limitation:** nothing periodically re-signals "still doesn't exist" *after* that
one guaranteed initial sync. This is inherent to Kubernetes watch semantics and is accepted here
rather than worked around with a separate store-driven sweep - `ReasonNotFound` is accurate as of the
last time `sync` ran for that key; it just won't be re-confirmed on any particular cadence while the
object stays absent.

## GVR resolution

A `ReadDesire`'s `Identity` carries `Group`+`Resource` only - no `Kind`/`Version` (unlike
`ApplyDesire`, which decodes a GVK from `KubeContent`); `TargetVersion` (see below) supplies the
missing piece explicitly, rather than it being guessed or resolved from the cluster's preferred
version. `resolveGVR` resolves the now fully-specified GVR via a single `IsNoMatchError` -> `Reset()` -> retry

**`resolveGVR` has two call sites, and a failure means something different at each:**

- `pollOnce` calls it once per desire per poll tick to decide `want` (see below). A failure here
  means this desire's target has never been confirmed resolvable at all, so it's reported as
  `Successful=False/PreCheckFailed` via `applyStatus` (same reason `applydesire` uses for an
  unmappable manifest - "the call could not be executed at all") - not just a log line. See
  "`sync`/`applyStatus`" below for why this has to be reported from `pollOnce` itself rather than
  from `sync`.
- `observeLive` (see "`TargetVersion`" below) calls it again, per-sync, but only on the
  object-level `TargetVersion`-mismatch path. At that point the GVR was already confirmed
  resolvable when the informer was started, so a failure here is a surprising, apparently-transient
  regression rather than an up-front precheck - it's reported as `KubeAPIError`, not
  `PreCheckFailed`.


**`pollOnce` distinguishes "desire still listed" (`seen`) from "GVR resolved this tick" (`want`) when
calling `InformerManager.Reconcile`, and this distinction is load-bearing, not cosmetic.** A
transient `resolveGVR` failure for a desire whose informer is already running must only skip
*starting a new* informer for it that tick - it must not tear down the existing, already-synced one.
`Reconcile` stops an informer only when its key is missing from `seen` (the desire is genuinely
gone); it starts one only when the key is present in `want` (a target actually resolved). Getting
this wrong (as an earlier version of this code did, keying the stop decision off `want` instead of
`seen`) means a single flaky discovery call needlessly destroys a healthy informer's cache, forcing
a full relist on the next successful tick.

## `TargetVersion`: explicit, required, and rebuild-on-change

A `ReadDesire`'s `TargetVersion` (paired with `Identity.Group`/`Identity.Resource`) is the
Kubernetes API version to observe - explicit and required, not resolved from the cluster's current
preferred version. `resolveGVR` passes it straight through to `ResourceFor` rather than guessing.
Unlike `Identity`, `TargetVersion` isn't part of the store's keying - it's an ordinary field on the
record, immutable in practice only because `SpecStore` has no `UpdateReadDesireSpec`; the only way
to change it is to delete and recreate the `ReadDesire`.

`trackedInformer` remembers the exact `schema.GroupVersionResource` it was built with.
`InformerManager.Reconcile`'s start loop compares that against the freshly resolved target for
each `want` key: if they match, it leaves the informer alone; if they differ (the desire's declared
`TargetVersion` changed since the informer was built), it tears down the stale informer and starts
a fresh one on the new GVR. Earlier versions of this code only checked "does an informer already
exist for this key" and never compared GVRs, so a `TargetVersion` change could never take effect -
the informer would silently keep watching the old version forever.

A version mismatch can also surface at the object level: `observe` compares the live object's
actual `GroupVersionKind().Version` against the desire's declared `TargetVersion` after a
successful lister read. Generic client-side version conversion isn't possible here -
`unstructured.Unstructured`/dynamic objects have no compiled Go type to convert through via
`runtime.Scheme`, unlike `applydesire`'s typed manifests - so rather than guess, a mismatch here
triggers `observeLive`: a direct `Get` against `c.dyn` (bypassing the cache entirely) at the GVR
`resolveGVR` resolves for `targetVersion`. In practice this means the informer is still watching a
version the `ReadDesire` no longer declares - e.g. a brief race while `Reconcile` is mid-rebuild
after the desire was deleted and recreated with a different `TargetVersion`, and the old informer
hasn't been torn down and replaced yet. `observeLive` distinguishes two cases that look identical
from the cache alone:

- **Transient cache staleness** (the rebuild race above) - the live object agrees with
  `targetVersion`. Since the round trip is already paid for, its content is used directly for
  `synced(...)` rather than discarded; the cache catches up on its own via the next informer
  event/resync regardless.
- **A genuine, persistent problem** (misconfigured `TargetVersion`, or a real bug) - the live
  object still disagrees, or the `Get` itself fails (including a `resolveGVR` failure inside
  `observeLive`, or the object being genuinely gone - `NotFound`). This is no longer explainable as
  cache lag, so it's escalated to a real `KubeAPIError` (or `NotFound`) status write, unlike the
  cache-only check which never reaches status at all.

## Informer lifecycle (`InformerManager`)

`dynamicinformer`'s shared-factory `Start`/`Shutdown` operate on every registered GVR at once - there
is no per-GVR (or per-desire) stop at the factory level. `InformerManager` instead builds each
informer directly via `dynamicinformer.NewFilteredDynamicInformer` and calls `informer.Run(stopCh)`
with its own dedicated stop channel per desire, so one desire's informer can be torn down
independently of every other.

Each newly-started informer's `cache.WaitForCacheSync` is awaited in its own background goroutine,
not inline in `Reconcile` - starting several new informers in one poll tick must not serialize on
each other's initial `List` completing. `InformerManager.Lister(key)` exposes the tracked
`cache.GenericLister` so `sync` can read the cached object without an apiserver round trip.

## `sync`/`applyStatus`: fetch, compute, suppress, persist

`applyStatus(ctx, id, compute)` is the one place status is ever written: fetch, call `compute` with
the current status, compare, and persist if it changed. Both `sync` (per-key, workqueue-driven -
`compute` is `observe`) and `pollOnce` (per-tick, for a desire whose GVR didn't resolve at all -
`compute` is `preCheckFailed`) go through it. This mirrors `applydesire.applyToCluster`, where every
outcome - including a mapping failure - produces a status through the same code path, rather than
being handled as a special case; the difference is that `readdesire` needs *two* call sites into
that shared path, because GVR resolution and per-object observation happen in different places
(`pollOnce` vs. `sync`), unlike `applydesire` where both happen inside one function.

1. `GetReadDesire` - if `ErrNotFound`, the desire was deleted since it was looked up; return `nil`,
   nothing to do (its informer, if any, will be torn down on the next poll tick regardless).
2. `observe`/`observeLive` - read the object via `InformerManager.Lister(key).ByNamespace(...).Get(...)`,
   falling back to a live `Get` on a `TargetVersion` mismatch (see above). No informer yet, or a
   `NotFound` miss (cached or live), records `ReasonNotFound`. Any other read error, a JSON marshal
   failure, or an unresolved fallback GVR records `ReasonKubeAPIError` and preserves the previous
   `KubeContent` (a transient read failure shouldn't erase the last known good mirror). Otherwise
   records `ReasonSynced` with the freshly marshaled object as `KubeContent`.
3. `readStatusEqual` compares the freshly observed status against the fetched desire's current
   status (conditions *and* semantically canonicalized `KubeContent` - `util.Equal` alone doesn't
   cover the latter). The canonical comparison removes Kubernetes-managed
   `metadata.managedFields`, `metadata.resourceVersion`, and `metadata.generation`; raw observed
   content is still what gets persisted. It skips the write entirely if nothing changed.
   `UpdateReadDesireStatus` is still a real Redis `WATCH`/`MULTI`/`EXEC` write - an unconditional
   write on every 60s resync tick would still generate needless Redis load and replication traffic
   for objects that haven't actually changed, so the no-op check earns its keep on that basis.
4. `UpdateReadDesireStatus(ctx, id, newStatus)` - per `pkg/desire/AGENTS.md`, each desire type is
   its own independent record with its own `Version` now, so a Read status write has no
   cross-desire-type CAS concern to get wrong the way it would if Apply/Delete/Read still shared one
   physical record's version.

## What reaches `ReadDesire.Status` vs. what's log-only

Reported to status (via `applyStatus`): `observe`/`observeLive`'s outcomes
(`Synced`/`NotFound`/`KubeAPIError`) and `pollOnce`'s `resolveGVR` failures (`PreCheckFailed`).
Log-only, by necessity rather than oversight: `ListReadDesires` failing in `pollOnce`
(partition-wide, not attributable to one desire - same as `applydesire.RunOnce`'s own
`ListApplyDesires` failure), `GetReadDesire` failing inside `applyStatus` (nothing to compute a new
status against if the read itself failed). `UpdateReadDesireStatus` itself failing cannot be written
into the status update that failed; it increments the process-local `failed` write counter and is
returned through the workqueue retry path when called by `sync`; `processNextWorkItem` then uses
`AddRateLimited`. When called by `pollOnce`, the failure is logged and the next poll can retry it;
`pollOnce` and `InformerManager.Reconcile` do not call `queue.Add` themselves. The controller also
tracks process-local `skipped` and `performed` write counters; Prometheus exposure is deferred to
HYPERFLEET-1722.

## Operational semantics (MVP)

- **Fixed tuning constants:** `resyncPeriod` (60s, informer resync) and `workerCount` (4, worker
  pool size) are unexported consts, not configurable. `pollInterval` is the one caller-supplied
  knob (`New`'s last parameter), mirroring the POC Helm chart's `applier.pollInterval`.
- **No per-key dedup beyond the workqueue's own semantics:** adding an already-queued (or
  in-flight) key just marks it dirty for reprocessing once - see client-go `workqueue`'s own
  documented behavior.
- **A `NotFound` reason has no separate revalidation cadence** beyond whatever event/resync
  eventually fires for that desire's informer - see "Poll loop" above.
- **Per-GVR/per-desire informer restart on transient list/watch errors** relies entirely on the
  reflector's own built-in retry (client-go `cache.Reflector`); this package layers nothing
  additional on top.
