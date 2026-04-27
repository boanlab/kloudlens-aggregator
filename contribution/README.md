# Contribution Guide

This guide describes how to set up a development environment for
`kloudlens-aggregator` and submit changes.

## Prerequisites

- Go 1.24+
- Docker (for building container images)
- A KloudLens working tree at `../../KloudLens` (the local-dev `replace`
  in `go.mod` resolves the shared `api/pb` protobuf surface from there)
- `kubectl` and access to a Kubernetes cluster (only needed to exercise
  the EndpointSlice discovery path)
- `golangci-lint` and `gosec` (auto-installed by `make` if missing)

## Development Workflow

All Go source code lives under the `kloudlens-aggregator/` subdirectory.

```bash
cd kloudlens-aggregator

make build           # gofmt + vet + compiles bin/kloudlens-aggregator
make verify          # gofmt + vet + golangci-lint + gosec + test (CI gate)

# Run individual checks
make gofmt
make vet
make golangci-lint
make gosec
make test
```

## Container Image

```bash
cd kloudlens-aggregator
make build-image TAG=<tag>
make push-image  TAG=<tag>   # requires registry auth
```

Note: container builds depend on `github.com/boanlab/kloudlens` being
available via the Go module proxy. Until KloudLens cuts a tagged
release, image builds require dropping the `replace` directive and
substituting a pinned version.

## Updating the KloudLens Dependency

The aggregator pulls `pb`, `EventService`, and the wire types from the
[KloudLens](https://github.com/boanlab/KloudLens) repo's `api/pb`
package. To bump the pinned version (post-release):

```bash
cd kloudlens-aggregator
go get github.com/boanlab/kloudlens@<version>
go mod tidy
```

## Submitting Changes

1. Fork and create a feature branch (`feature/<topic>` or `fix/<topic>`).
2. Run `make verify` from `kloudlens-aggregator/` before pushing.
3. Open a pull request against `main`. Link related issues.
4. CI runs `gofmt`, `vet`, `golangci-lint`, `gosec`, and unit tests.

## Commit Message Convention

```
<type>(<scope>): <subject>

<body>
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`.

---

Copyright 2026 [BoanLab](https://boanlab.com) @ Dankook University
