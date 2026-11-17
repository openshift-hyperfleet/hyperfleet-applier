# HyperFleet Applier

Go library for the HyperFleet **desire contract** and store backends. A desire is
the declarative intent for a single Kubernetes resource on a management cluster
(apply, delete, or read). Store backends persist that intent and its status so
appliers can reconcile resources without ManifestWork-style bulk transport.

It collaborates with other HyperFleet components:

* **[API](https://github.com/openshift-hyperfleet/hyperfleet-api)** is the source of truth for fleet resource desired state and aggregated status
* **[Sentinel](https://github.com/openshift-hyperfleet/hyperfleet-sentinel)** polls the API and publishes CloudEvents that drive reconciliation
* **[Adapter](https://github.com/openshift-hyperfleet/hyperfleet-adapter)** listens for those events, applies changes, and reports status back to the API

## Quick Start

### Try Locally

**Prerequisites:** Go 1.26+, Make

```bash
make build
make test
make lint
```

Run `make help` for the full list of targets.

### Run the applier

The binary follows the same command and configuration conventions as the other HyperFleet services:

```bash
make run
```

Local runs are not in a pod, so the election namespace comes from
`POD_NAMESPACE`; the Makefile defaults it to `hyperfleet`. Override it with
`make run POD_NAMESPACE=<namespace>`.

Override the default configuration path with `make run CONFIG=/path/to/applier.yaml`. The shared
Kubernetes discovery cache is invalidated using `discovery_refresh_interval`, allowing newly
installed CRDs to be reconciled without restarting the applier. All three controllers use
`poll_interval`.

`make run` uses `$KUBECONFIG` when set, otherwise `$HOME/.kube/config`. Override it with
`make run KUBE_CONFIG_PATH=/path/to/kubeconfig`. Redis must already be reachable at the URL in the
configuration file (or supplied through `HYPERFLEET_REDIS_URL`).

Configuration precedence follows the HyperFleet convention: command-line flags, then
`HYPERFLEET_*` environment variables, then YAML, then defaults. Use
`bin/hyperfleet-applier config-dump --config configs/applier.yaml` to inspect the merged,
redacted configuration.

Leader election is always enabled: only the lease holder runs the apply,
delete, and read loops, and standby replicas wait. Namespace resolution, lease
timings, failover, and deployment constraints are documented in
[Replica model and leader election](charts/README.md#replica-model-and-leader-election).

### Package layout

| Package | Description |
|---------|-------------|
| [`pkg/desire`](pkg/desire) | Desire types (`ApplyDesire`, `DeleteDesire`, `ReadDesire`), identity validation, `SpecStore` / `StatusStore` interfaces |
| [`pkg/desire/store/memory`](pkg/desire/store/memory) | In-memory store for unit tests |
| [`pkg/desire/store/redis`](pkg/desire/store/redis) | Redis-backed store with WATCH/MULTI/EXEC CAS |
| [`pkg/desire/store/conformance`](pkg/desire/store/conformance) | Shared conformance suite exercised by both backends |

```bash
go get github.com/openshift-hyperfleet/hyperfleet-applier
```

## Documentation

### For Developers

| Resource | Description |
|----------|-------------|
| `make help` | Build, test, lint, format, and verify targets |
| [`pkg/desire` package docs](pkg/desire/doc.go) | Desire model, identity rules, and store contracts |

### Architecture

| Resource | Description |
|----------|-------------|
| [HyperFleet Architecture](https://github.com/openshift-hyperfleet/architecture) | System design |
| [HyperFleet API Spec](https://github.com/openshift-hyperfleet/hyperfleet-api-spec) | API contract |
| [Broker Library](https://github.com/openshift-hyperfleet/hyperfleet-broker) | Messaging abstraction |
| [Infrastructure](https://github.com/openshift-hyperfleet/hyperfleet-infra) | Deployment automation |

## Security posture

Credentials are stored scoped to the applier's own partition by convention.
Cryptographic enforcement of that scoping arrives with the production backends.

### FIPS compliance

The production image builds with Go's native FIPS 140-3 module (`GOFIPS140=v1.0.0`). The `godebug fips140=on` directive in `go.mod` enables FIPS mode by default, including on the Red Hat Go toolset (which does not enable it merely by selecting the module). `GOEXPERIMENT=noboringcrypto` keeps builds compatible with toolchains where the legacy experiment was saved with `go env -w`. The Dockerfile checks both the selected module and `DefaultGODEBUG=fips140=on`. See [Go's FIPS 140-3 documentation](https://go.dev/doc/security/fips140) and [HYPERFLEET-1940](https://redhat.atlassian.net/browse/HYPERFLEET-1940).

Red Hat Go toolsets use an OpenSSL crypto backend by default on Linux with cgo, and that backend aborts at startup on FIPS-enabled hosts once a `fips140` GODEBUG value is set. The Makefile therefore adds `-tags no_openssl` on those toolchains to select Go's native backend, and the Dockerfile rejects a binary whose build info does not record the tag.

FIPS production deployments must not disable that compiled default via `GODEBUG=fips140=off` or `GODEBUG=fips140=` (including within comma-separated values), a `godebug` directive in `go.work`, or a `//go:debug` directive in the main package. The Dockerfile checks the compiled default, not runtime overrides; an entirely empty or unset `GODEBUG` preserves the compiled default.

## RBAC allowlist

The applier's ClusterRole uses an explicit GVR allowlist.
See [Deriving the RBAC allowlist](charts/README.md#deriving-the-rbac-allowlist) in the
chart README for how to compute and configure it.

## Contributing

1. Verify you're a member of the `openshift-hyperfleet` organization
2. Confirm you're added to the hyperfleet team
3. Code reviews and approvals are managed through the OWNERS file

For access issues, contact a repository administrator or organization owner.

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
