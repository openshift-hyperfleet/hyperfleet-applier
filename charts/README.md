# hyperfleet-applier

![Version: 0.2.0](https://img.shields.io/badge/Version-0.2.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.0.0-dev](https://img.shields.io/badge/AppVersion-0.0.0--dev-informational?style=flat-square)
HyperFleet Applier - Kubernetes controller for reconciling ApplyDesire and DeleteDesire resources
**Homepage:** <https://github.com/openshift-hyperfleet/hyperfleet-applier>
## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| HyperFleet Team | <hyperfleet-team@redhat.com> | <https://github.com/openshift-hyperfleet> |
## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| imagePullSecrets | list | `[]` | List of image pull secrets to use for pulling container images |
| nameOverride | string | `""` | Override the chart name |
| fullnameOverride | string | `""` | Override the full release name |
| serviceAccount.create | bool | `true` | Create a service account for the controller |
| serviceAccount.annotations | object | `{}` | Annotations to add to the service account |
| serviceAccount.name | string | `""` | Override the service account name |
| rbac.create | bool | `true` | Create reconciliation ClusterRole/ClusterRoleBinding and leader-election Role/RoleBinding |
| rbac.allowlist | list | `[]` | Explicit allowlist of API group + resource pairs the applier's ServiceAccount may manage on this cluster. Each entry must specify `apiGroups` and `resources`; verbs (get, list, watch, create, patch, delete) are added automatically by the template. Do NOT add a "*" apiGroup or resource here -- see rbac.devModeWildcard for the (off-by-default) escape hatch. This list must be tailored per Helm release/management cluster; see "Deriving the RBAC allowlist" in the chart README for how to compute it. |
| rbac.devModeWildcard | bool | `false` | DANGER: replaces rbac.allowlist with a cluster-wide "*"/"*" ClusterRole rule when true. This disables the allowlist entirely and grants unrestricted access to every resource type in the cluster. NEVER enable this in a shared, staging, or production cluster; add the required resource types to rbac.allowlist instead. Default: false (must be explicitly opted into). |
| podAnnotations | object | `{}` | Annotations to add to controller pods |
| podLabels | object | `{}` | Labels to add to controller pods |
| podSecurityContext | object | `{"runAsNonRoot":true,"seccompProfile":{"type":"RuntimeDefault"}}` | Pod-level security context |
| securityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"readOnlyRootFilesystem":true,"runAsNonRoot":true}` | Container-level security context |
| resources | object | `{"limits":{"cpu":"500m","memory":"512Mi"},"requests":{"cpu":"100m","memory":"128Mi"}}` | Resource requests and limits for the controller container |
| replicaCount | int | `1` | Number of replicas; one leader reconciles and the others wait on standby |
| image.registry | string | `""` | Container image registry (required) |
| image.repository | string | `""` | Container image repository (required) |
| image.tag | string | `""` | Container image tag (required) |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy |
| config.managementCluster | string | `""` | Management cluster identifier - must match the partition this applier instance manages (required) |
| config.pollInterval | string | `"1m"` | Polling interval for reconciliation loops (e.g., "5s", "1m") |
| config.discoveryRefreshInterval | string | `"30s"` | Interval between Kubernetes discovery cache refreshes |
| config.redis.url | string | `""` | Redis connection URL |
| config.log.level | string | `"info"` | Log level (e.g., "info", "debug", "warn", "error") |
| config.log.format | string | `"json"` | Log format ("json" or "text") |
| config.log.output | string | `"stdout"` | Log output destination ("stdout" or "stderr") |
| config.kubeConfigPath | string | `""` |  |
| configOverride | string | `""` | Raw config.yaml content that replaces the template-generated config.yaml configOverride must have the required values management_cluster, poll_interval, discovery_refresh_interval, and clients.redis.url when set, default values in config are ignored Example: --set-file configOverride=configs/applier.yaml |

## Replica model and leader election

Leader election is always enabled, including with `replicaCount: 1`. Only the
leader runs the serial apply-then-delete loop and the ReadDesire controller.
Standby replicas refresh discovery but do not reconcile desires. This also
protects overlapping pods during rolling updates; extra replicas provide
availability, not additional reconciliation throughput.

The Lease name is a SHA-256 hash of `management_cluster`, prefixed with
`hyperfleet-applier-`. Its namespace is the pod's own: `POD_NAMESPACE` wins
when set, otherwise the applier reads the namespace Kubernetes projects into
the service-account volume. All replicas and releases managing the same
partition must resolve to the same namespace with the same management-cluster
identifier.

The Lease duration is 120 seconds, renewal deadline 60 seconds, and retry period
10 seconds. The 60-second gap exceeds the existing 30-second SSA and DELETE
timeouts. On renewal failure, the leader cancels its controllers, waits for
them to stop, and exits. A deliberate exit - graceful shutdown or a controller
failure - attempts the hand-over after the controllers drain, so a restart or
rolling update normally takes over within about one retry period instead of
waiting out the expiry. The attempt is best-effort: when its Lease read or
update fails, the record keeps the previous holder and duration, and the
successor waits for the 120-second expiry plus an acquisition retry. Crashes,
freezes, and kills never run the hand-over at all, so an ungraceful death
waits out the same expiry. Failure detection and scheduling can add delay;
120 seconds is not a strict end-to-end failover bound. A new leader starts
reconciliation immediately.

Lease election is not fencing against arbitrary process pauses: cancellation
prevents subsequent writes but cannot undo a request already accepted by the
API server. The timeout margin assumes responsive processes and bounded calls.
To also cover a leader that stalls past the lease duration and resumes -
after another replica may have taken over, but before the client-go elector
gives up - every SSA and DELETE call is additionally gated on lease
freshness: the leader's last successful lease renewal must be younger than
the lease duration minus one retry period. When it is not, the write is
refused, the controllers' context is canceled, and the process exits exactly
as it does on renewal failure. Each write context is also capped at the last
successful renewal plus that bound, so a process paused between the check
and the request resumes to an expired write context instead of sending it,
and a request already in flight is aborted at the bound.

The chart creates a namespaced Role/RoleBinding granting `get`, `create`, and
`update` on Leases, independently of the reconciliation allowlist. With
`rbac.create: false`, provision these permissions yourself.

The election namespace is the pod's own namespace — there is no dedicated
configuration for it; `POD_NAMESPACE` is the only override. Outside a pod
(local execution), set `POD_NAMESPACE` yourself; the applier needs `get`,
`create`, and `update` on Leases in that namespace. Missing both
`POD_NAMESPACE` and the projected namespace file is a startup error.

## Deriving the RBAC allowlist

The chart's `rbac.allowlist` must list every API group + resource pair
that adapters configured for this management cluster will ask the applier to reconcile.
Each adapter task config declares the resource kinds it produces desires for — collect
them into one allowlist per cluster. The template automatically adds the full verb set
(get, list, watch, create, patch, delete) to each entry.

1. For each adapter targeting this applier's `managementCluster`, inspect its task
   configuration and note every `apiGroup` + `resource` pair it can emit during
   the resources phase.
2. Determine the representative `apiGroup` + `resource` pair for each `apiVersion` + `kind`.
3. Merge the pairs into `rbac.allowlist` entries in your Helm values override.
   Each entry needs only `apiGroups` and `resources` — verbs are templated automatically.
4. Run `helm template` and verify the rendered ClusterRole contains exactly the rules
   you expect.

### Example: adapter task config to allowlist

Each k8s resource in an adapter task config maps to one allowlist entry.
Determine the API group from apiVersion. Determine the REST resource
name using Kubernetes API discovery (kubectl api-resources);
resource names cannot in general be derived from kind.

Learn more about:
`apiGroup` ->  https://kubernetes.io/docs/reference/using-api
`kubectl api-resources` -> https://kubernetes.io/docs/reference/kubectl/generated/kubectl_api-resources

```yaml
# Adapter task config (resources section)
resources:
  - name: "resource0"
    transport:
      client: "kubernetes"
    manifest:
      apiVersion: v1
      kind: ConfigMap
      ...
```

This maps to:

```yaml
# Applier values override
rbac:
  allowlist:
    - apiGroups: [""]            # core group (apiVersion: v1)
      resources: ["configmaps"]  # resource of ConfigMap kind
```

For local development, `rbac.devModeWildcard=true` grants unrestricted permissions for every resource.
