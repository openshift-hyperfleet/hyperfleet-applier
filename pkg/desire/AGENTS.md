# pkg/desire

Desire types, identity/validation rules, and the SpecStore/StatusStore contracts. Backends
implementing these contracts live in `pkg/desire/store/` (see `pkg/desire/store/AGENTS.md`).

## Desire model

Three desire types, each targeting exactly one Kubernetes resource via an `Identity`
(managementCluster + type + group/resource/namespace/name):

- **ApplyDesire** — make a resource exist with specific content (SSA, `Force=true`)
- **DeleteDesire** — make a resource not exist (confirmed gone past finalizers)
- **ReadDesire** — mirror a live object's state back to the control plane (includes `KubeContent`)

Each desire type is its own record with its own `Version`, keyed by full `Identity`. A target can
therefore have up to three sibling records: apply, delete, and read.

Each record also has `Generation`, initialized to 1 on creation. A successful
`UpdateApplyDesireSpec` increments it only when the desired JSON content changes and leaves the
previous status untouched; equivalent JSON with different whitespace or object key order does
not increment it. Status writes never change it. `Version` remains the CAS token and advances on
every accepted Apply spec update, even if the content is unchanged, and can advance on status
writes. For every desire type, compare the `Successful` condition's `ObservedGeneration` with the
desire's `Generation` in the same returned record: a mismatch means the condition describes an
older spec; a match means its status and reason describe an attempt at the current spec, not
necessarily success or workload readiness. A missing condition supplies no reported outcome.
Delete and Read desires are assigned generation 1 by the store and have no spec-update operation,
so it never advances, but their controllers still stamp it into `ObservedGeneration` on every
outcome.

### Generation boundaries

These values identify different things and must not be compared across rows:

| Value | Owner and meaning |
| --- | --- |
| Desire `Generation` / desire condition `ObservedGeneration` | The store's revision of one desire's spec / the revision processed by that desire's controller. |
| `hyperfleet.io/generation` inside `KubeContent` | The originating HyperFleet API resource's generation, carried by the adapter on the manifest. |
| Live object's `metadata.generation` / its own observed-generation status | The target Kubernetes API and workload controller's revision and observation of that Kubernetes object. |

For example, a new ApplyDesire can have generation **1**, contain API annotation **7**, and
target a Kubernetes object at generation **3**. Its Apply condition stamps **1**. Each desire
has an independent counter; sibling Apply/Delete/Read generations are not comparable, and
deletion followed by recreation starts a new counter at 1.

[HYPERFLEET-1441](https://redhat.atlassian.net/browse/HYPERFLEET-1441) covers the adapter's
annotation-based create/update/skip decisions. It does not consume the desire's `Generation`
or its condition's `ObservedGeneration`. HYPERFLEET-1516 supplies that correlation metadata;
adapter policy consuming it is separate work in the scope of
[HYPERFLEET-1419](https://redhat.atlassian.net/browse/HYPERFLEET-1419), not implemented here.

### Ownership and status

Create-time invariants across sibling records:

- one owner per target (`ErrOwnerConflict` on mismatch)
- apply/delete mutual exclusion (`DeleteDesire` supersedes `ApplyDesire`; active delete blocks apply)
- `ReadDesire` coexists with either

`Identity` implements `slog.LogValuer` so controllers can log `"identity", id` instead of unwrapping
fields by hand (shared across apply/delete/read).

`TypeSuccessful` names the summary reconciliation condition. Interpret its `Status` and `Reason`
together: `Applied` records acceptance of server-side apply, `Deleted` confirms absence, and
`Synced` reports a mirrored object. `Successful=False` covers both in-progress and failure
states, distinguished by `Reason`.

## Store contracts

Store contracts split spec and status:

- **SpecStore** — Create/Get/Update/Delete per desire type, plus `ListApplyDesires` /
  `ListDeleteDesires` / `ListReadDesires` and `DeleteByPrefix`. Create/Update enforce single-writer
  ownership (`ErrOwnerConflict` on mismatch) and require exact `Version` match for updates
  (`ErrVersionConflict` if stale). The `owner` argument to `UpdateApplyDesireSpec` is checked,
  not stored as a new owner.
- **StatusStore** - status-only Get/Update per desire type. Does **not** check ownership.
  `UpdateApplyDesireStatus` and `UpdateDeleteDesireStatus` require exact `Version` match;
  `UpdateReadDesireStatus` does not advance `Version`.

The spec/status split is a Go capability boundary, not a storage-enforced permission boundary.

`Validate()` methods on `Identity`/`ApplySpec`/each desire type enforce DNS-1123/1035 sizing rules
on identity fields (`validate.go`) — backends call these at Create time.

`observe.go` holds process-local owner-conflict metrics/logging.
