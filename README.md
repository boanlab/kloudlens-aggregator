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
- NDJSON sink to stdout / stderr / file.
- Optional re-export gRPC server for downstream subscribers.
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
