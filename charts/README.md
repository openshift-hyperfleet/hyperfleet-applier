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
| rbac.create | bool | `true` | Create RBAC resources (ClusterRole, ClusterRoleBinding) |
| rbac.allowlist | list | `[]` | Explicit allowlist of API group + resource pairs the applier's ServiceAccount may manage on this cluster. Each entry must specify `apiGroups` and `resources`; verbs (get, list, watch, create, patch, delete) are added automatically by the template. Do NOT add a "*" apiGroup or resource here -- see rbac.devModeWildcard for the (off-by-default) escape hatch. This list must be tailored per Helm release/management cluster; see "Deriving the RBAC allowlist" in the chart README for how to compute it. |
| rbac.devModeWildcard | bool | `false` | DANGER: replaces rbac.allowlist with a cluster-wide "*"/"*" ClusterRole rule when true. This disables the allowlist entirely and grants unrestricted access to every resource type in the cluster. NEVER enable this in a shared, staging, or production environment -- it exists only to unblock local/dev iteration against a resource kind not yet added to rbac.allowlist. Default: false (must be explicitly opted into). |
| podAnnotations | object | `{}` | Annotations to add to controller pods |
| podLabels | object | `{}` | Labels to add to controller pods |
| podSecurityContext | object | `{"runAsNonRoot":true,"seccompProfile":{"type":"RuntimeDefault"}}` | Pod-level security context |
| securityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"readOnlyRootFilesystem":true,"runAsNonRoot":true}` | Container-level security context |
| resources | object | `{"limits":{"cpu":"500m","memory":"512Mi"},"requests":{"cpu":"100m","memory":"128Mi"}}` | Resource requests and limits for the controller container |
| replicaCount | int | `1` | Number of controller replicas to run |
| image.registry | string | `""` | Container image registry (required) |
| image.repository | string | `""` | Container image repository (required) |
| image.tag | string | `""` | Container image tag (required) |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy |
| applier.managementCluster | string | `""` | Management cluster identifier - must match the partition this applier instance manages (required) |
| applier.pollInterval | string | `""` | Polling interval for reconciliation loops (e.g., "5s", "1m") (required) |
| redis.address | string | `""` | Redis server address in format "host:port" (required) |

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
