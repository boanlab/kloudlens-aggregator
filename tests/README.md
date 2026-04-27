# tests/ — kloudlens-aggregator live e2e

Deploys the aggregator into a real Kubernetes cluster alongside the
upstream KloudLens core (CRDs + DaemonSet, pulled from GitHub) and
exercises the operator-facing surface:

- `/healthz`, `/readyz` return `ok`
- `/metrics` serves the Prometheus exposition format
- `/stats` returns the plaintext counters line
- The aggregator's logs report `k8s discovery watching <ns>/<svc>`,
  proving the EndpointSlice watch is wired up

## Prerequisites

- A reachable Kubernetes cluster (`kubectl cluster-info` works)
- `docker`, `curl`, `jq`, `sudo`, `ctr` (containerd CLI) on PATH
- Passwordless sudo for `ctr -n k8s.io images import` (the image is
  side-loaded into the kubelet's image store rather than pushed)
- The build host should also be a node where the KloudLens DaemonSet
  runs — otherwise `kubectl port-forward` is the only path the harness
  uses and that works from anywhere

## Run

```bash
make up      # apply KloudLens core + this aggregator
make check   # assert /healthz, /metrics, /stats, discovery
make down    # tear down
make all     # up + check
```

A run report is written to `tests/artifacts/report.md`.

## Knobs (env vars)

| variable | default | meaning |
|---|---|---|
| `KL_REF` | `main` | GitHub ref to fetch KloudLens manifests from |
| `KL_NS` | `kloudlens` | Namespace the aggregator + agent share |
| `SELF_TAG` | `latest` | Image tag built and applied |
| `METRICS_PORT` | `9091` | Container port the aggregator serves on |
