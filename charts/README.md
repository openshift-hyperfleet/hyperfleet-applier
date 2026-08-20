# hyperfleet-applier

![Version: 0.1.0](https://img.shields.io/badge/Version-0.1.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.0.0-dev](https://img.shields.io/badge/AppVersion-0.0.0--dev-informational?style=flat-square)

HyperFleet Applier - Kubernetes controller for reconciling ApplyDesire and DeleteDesire resources

**Homepage:** <https://github.com/openshift-hyperfleet/hyperfleet-applier>

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| HyperFleet Team |  |  |

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| imagePullSecrets | list | `[]` | List of image pull secrets to use for pulling container images |
| nameOverride | string | `""` | Override the chart name |
| fullnameOverride | string | `""` | Override the full release name |
| serviceAccount.create | bool | `true` | Create a service account for the controller |
| serviceAccount.automount | bool | `true` | Automatically mount service account token |
| serviceAccount.annotations | object | `{}` | Annotations to add to the service account |
| serviceAccount.name | string | `""` | Override the service account name |
| rbac.create | bool | `true` | Create RBAC resources (ClusterRole, ClusterRoleBinding) |
| rbac.rules | list | `[{"apiGroups":[""],"resources":["configmaps","secrets","services","serviceaccounts"],"verbs":["get","list","watch","create","update","patch","delete"]},{"apiGroups":["apps"],"resources":["deployments","statefulsets","daemonsets"],"verbs":["get","list","watch","create","update","patch","delete"]},{"apiGroups":[""],"resources":["events"],"verbs":["create","patch"]}]` | ClusterRole rules - the controller needs broad permissions to apply any resource type |
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
| redis.enabled | bool | `true` | Enable Redis store configuration |
| redis.address | string | `""` | Redis server address in format "host:port" (required) |

