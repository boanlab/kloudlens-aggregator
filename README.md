# kloudlens-aggregator

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Version](https://img.shields.io/badge/Go-1.24%2B-blue.svg)](https://golang.org/)

`kloudlens-aggregator` is the optional cluster fan-in for KloudLens. It
subscribes to every node's `kloudlens` agent (the `EventService` gRPC
endpoint) and merges their envelopes into a single NDJSON sink, with an
optional re-export gRPC surface for downstream SIEM / tier-2 consumers.

Sister repositories:

- [`boanlab/KloudLens`](https://github.com/boanlab/KloudLens) — the
  on-node agent (eBPF + intent/graph/baseline/contract pipeline + gRPC).
- [`boanlab/kloudlens-cli`](https://github.com/boanlab/kloudlens-cli) —
  the operator CLI (`klctl`).

## When to use it

In a multi-node cluster, every `kloudlens` agent exposes its own gRPC
`EventService`. A consumer that wants events from all nodes would
otherwise need to maintain N connections. The aggregator presents a
single endpoint that fans in all node streams and re-exports them, with
EndpointSlice-based discovery so the upstream set tracks DaemonSet
rolls automatically.

## Features

- Static `--agents=host:port,...` plus EndpointSlice discovery for
  dynamic agent rosters (`--k8s-service=NAMESPACE/SERVICE`).
- Per-(agent, stream) cursor persistence — restarts resume losslessly.
- Parallel envelope WAL (size + TTL retention) feeding the re-export
  gRPC surface so downstream subscribers survive aggregator restarts.
- Cluster-wide peer attribution: joins a connect to the remote process,
  pod, and node across agents, via exact address, EndpointSlice, Service
  VIP, or wildcard bind. Listeners enter the registry on
  `ListenerAdvertise` and leave on `ListenerWithdraw` when the binding
  process exits, so an address reused by a new process is never
  attributed to its predecessor. A 30s TTL is the backstop for an agent
  that crashes or partitions before its withdraw arrives.
- NDJSON sink to stdout / stderr / file.
- Optional re-export gRPC server for downstream subscribers, honouring the
  subscription's kind, namespace, pod, and minimum-severity filter on both
  the live tail and the WAL replay.
- `/healthz`, `/readyz`, `/metrics`, `/stats` on `--metrics-addr`.

## Prerequisites

- Go 1.24+
- One or more `kloudlens` agents reachable over gRPC

## Build

```bash
make build           # gofmt + vet + build → bin/kloudlens-aggregator
./bin/kloudlens-aggregator --help
```

The KloudLens dependency is fetched normally through the Go module
proxy — no sibling checkout is required.

### Container Image

```bash
make build-image TAG=<tag>
make push-image  TAG=<tag>
```

## Running locally

```bash
./bin/kloudlens-aggregator \
  --agents=127.0.0.1:8181 \
  --metrics-addr=:9091 \
  --wal-dir=/tmp/kloudlens-aggregator-wal \
  --reexport-addr=:9450
```

In a Kubernetes cluster, point `--k8s-service=NAMESPACE/SERVICE` at the
headless Service backing the `kloudlens` DaemonSet — the aggregator
watches `EndpointSlice` and re-subscribes as nodes roll. A ready-to-apply
manifest lives at [`deployments/kloudlens-aggregator.yaml`](deployments/kloudlens-aggregator.yaml).

Cluster-wide RBAC (`get`/`list`/`watch`) is required on `endpointslices`
plus `pods` and `services`. EndpointSlice watch discovers the agents and
maps a pod IP to its identity; pods and services back Service-VIP
resolution. Without them the aggregator still runs, but every VIP or
wildcard connect degrades to an unattributed flow.

## Flags

| Flag | Default | Purpose |
|---|---|---|
| `--agents` | *(required)* | Comma-separated agent addresses, `host:port[,host:port…]`. Ignored when `--k8s-service` is set. |
| `--k8s-service` | *(empty)* | `namespace/name` of the headless Service backing the agent DaemonSet; the aggregator then watches EndpointSlice and re-subscribes as nodes roll. |
| `--agent-port` | `0` | Port for each discovered agent pod; `0` takes the first port in the EndpointSlice. |
| `--streams` | `intent` | Streams to subscribe to: `intent`, `deviation`, `graph_edge`, `lifecycle`, `audit`. |
| `--resolve-service-vip` | `true` | With `--k8s-service`, watch Services and EndpointSlices cluster-wide so a connect to a ClusterIP resolves to the backing workload. Needs cluster-scoped `get`/`list`/`watch` on `services` and `endpointslices`. |
| `--output` | `-` | NDJSON sink: `-` stdout, `stderr`, or a file path. |
| `--reexport-addr` | *(empty)* | `host:port` for the re-export EventService gRPC; empty disables it. |
| `--metrics-addr` | *(empty)* | `host:port` serving `/healthz`, `/readyz`, `/metrics`, `/stats`. |
| `--cursor-file` | *(empty)* | Per-(agent, stream) cursor JSON. Empty means no persistence, so a restart resumes from the live tail. |
| `--consumer-id` | `kloudlens-aggregator` | `consumer_id` sent on every Subscribe; the per-agent cursor is keyed on it. |
| `--aggregator-id` | `kloudlens-aggregator` | `NodeId` stamped on outgoing re-export cursors, so downstream can federate several clusters. |
| `--queue-depth` | `16384` | Internal fan-in channel size; envelopes are dropped when it is full (see `kloudlens_aggregator_dropped_total`). Sized to absorb burstiness, not sustained overload — if drops persist, the sink or WAL is the bottleneck rather than this buffer. |
| `--backoff` | `2s` | Retry delay between Subscribe reconnects on one agent. |
| `--wal-dir` | *(empty)* | WAL directory for merged envelopes. Empty means no WAL, and re-export then serves live subscribers only. |
| `--wal-max-bytes` | `2 GiB` | Soft cap on WAL retention; oldest segments are trimmed past it. |
| `--wal-segment-bytes` | `32 MiB` | WAL segment rotation threshold. |
| `--wal-ttl` | `2h` | WAL segment TTL, enforced on each GC tick. |
| `--wal-gc-interval` | `1m` | WAL janitor tick interval. |
| `--k8s-apiserver` | *(in-cluster)* | API server URL. |
| `--k8s-ca-file` | *(service account)* | CA bundle path. |
| `--k8s-token-file` | *(service account)* | ServiceAccount token path. |
| `--version` | | Print the build version and exit. |

## Streams

The aggregator subscribes to whichever streams `--streams=intent,...`
selects (default: `intent`). Each envelope is tagged with the
originating agent and an aggregator-assigned cluster sequence number,
then emitted to the NDJSON sink and (optionally) the re-export gRPC
surface.

## Verify (CI gate)

```bash
make verify          # gofmt + vet + golangci-lint + gosec + go test -race
```

## End-to-end test

`tests/test.sh` deploys KloudLens core (CRDs + DaemonSet) plus this
aggregator into a real cluster and exercises `/healthz`, `/metrics`,
and the NDJSON sink:

```bash
make -C tests up      # apply KloudLens core + this aggregator
make -C tests check   # assert health + metrics
make -C tests down    # tear down
```

See [`tests/README.md`](tests/README.md) for prerequisites and knobs.

## Project structure

```
.
├── cmd/kloudlens-aggregator/   # entrypoint (CLI flags, signal handling)
│   └── main.go
├── internal/aggregator/        # the aggregation logic
│   ├── aggregator.go           #   subscribe / fan-in / NDJSON emit
│   ├── cursorstore.go          #   per-(agent, stream) cursor persistence
│   ├── reexport.go             #   re-export gRPC server
│   ├── metrics.go              #   Prometheus collector for /metrics
│   ├── discovery/              #   EndpointSlice watcher
│   └── envwal/                 #   merged envelope WAL
├── deployments/                # in-cluster manifests
├── tests/                      # live e2e harness
├── Dockerfile                  # multi-stage; proxy-only build
├── Makefile                    # build / verify / image targets
└── go.mod                      # depends on kloudlens v0.1.x
```

## License

Licensed under the **Apache License 2.0** — see [LICENSE](LICENSE).

---

Copyright 2026 [BoanLab](https://boanlab.com) @ Dankook University
