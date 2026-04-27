# Security Policy

## Supported Versions

kloudlens-aggregator is currently at the v0.1.0 release line. Only the
latest commit on the `main` branch and the most recent tagged release
receive security fixes.

| Version | Supported |
|---|---|
| Latest `main` | Yes |
| v0.1.x | Yes |
| Forks / older commits | No |

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Please report security issues by email to **namjh@dankook.ac.kr** with
the subject line `[kloudlens-aggregator Security]`.

Include:

- A description of the vulnerability and its potential impact
- Steps to reproduce or a proof-of-concept
- Your environment (OS, Go version, Kubernetes version,
  kloudlens-aggregator commit SHA, KloudLens agent version it was
  paired with)
- Affected component (subscribe / fan-in loop, cursor store, envelope
  WAL, re-export gRPC, EndpointSlice discovery)
- Any suggested mitigations if you have them

We aim to acknowledge reports within 5 business days.

## Disclosure Policy

We follow a coordinated disclosure model. Please allow us reasonable
time to address the vulnerability before any public disclosure. We will
credit reporters in the release notes unless you prefer to remain
anonymous.

## Scope

In scope for this repo:

- The fan-in / subscribe loop, cursor store, envelope WAL, re-export
  gRPC server, and EndpointSlice discovery (`internal/aggregator/`)
- The CLI binary and its flag surface (`cmd/kloudlens-aggregator/`)
- The container image published from this repo

Out of scope — report against the relevant upstream instead:

- The KloudLens agent itself (`kloudlensd`, eBPF programs, gRPC wire
  format) — see
  [boanlab/KloudLens SECURITY.md](https://github.com/boanlab/KloudLens/blob/main/SECURITY.md)
- The `klctl` CLI — see
  [boanlab/kloudlens-cli SECURITY.md](https://github.com/boanlab/kloudlens-cli/blob/main/SECURITY.md)
- Third-party dependencies (report to the upstream project)
- Misconfigurations in user-supplied flags or cluster RBAC
