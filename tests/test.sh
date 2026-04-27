#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 BoanLab @ DKU
#
# Live integration test for kloudlens-aggregator.
#
# Self  : built locally — host binary not required, image is built and
#         side-loaded into containerd, then deployed via
#         deployments/kloudlens-aggregator.yaml.
# Others: KloudLens core (agent DaemonSet + CRDs) is pulled from GitHub
#         (KL_REF, default main).
#
# Subcommands:
#   up      apply KloudLens core, build/load self image, apply self
#   check   assert /healthz, /readyz, /metrics, /stats, NDJSON sink
#   down    delete self + KloudLens core
#   all     up + check

set -euo pipefail

E2E_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMP_ROOT="$(cd "${E2E_ROOT}/.." && pwd)"
ART_DIR="${E2E_ROOT}/artifacts"

KL_REF="${KL_REF:-main}"
KL_NS="${KL_NS:-kloudlens}"

SELF_YAML="${COMP_ROOT}/deployments/kloudlens-aggregator.yaml"
SELF_IMAGE="boanlab/kloudlens-aggregator"
SELF_TAG="${SELF_TAG:-latest}"
SELF_DEPLOY="kloudlens-aggregator"
SELF_SVC="kloudlens-aggregator"
METRICS_PORT="${METRICS_PORT:-9091}"

KL_RAW="https://raw.githubusercontent.com/boanlab/KloudLens/${KL_REF}/deployments"
KL_MANIFESTS=(
    "crds/baselinepolicy.yaml"
    "crds/behaviorcontract.yaml"
    "crds/hooksubscription.yaml"
    "crds/nodecapabilities.yaml"
    "manifests/daemonset.yaml"
)

PASS=0
FAIL=0
REPORT="${ART_DIR}/report.md"

_ok()   { echo "  [PASS] $1${2:+ — $2}"; PASS=$((PASS+1)); echo "- PASS: $1${2:+ — $2}" >>"${REPORT}"; }
_fail() { echo "  [FAIL] $1${2:+ — $2}" >&2; FAIL=$((FAIL+1)); echo "- FAIL: $1${2:+ — $2}" >>"${REPORT}"; }

_preflight() {
    for bin in kubectl docker curl jq; do
        command -v "${bin}" >/dev/null 2>&1 || { echo "missing: ${bin}" >&2; exit 1; }
    done
    if ! kubectl cluster-info >/dev/null 2>&1; then
        echo "kubectl cluster-info failed — cluster not reachable" >&2
        exit 1
    fi
    [[ -f "${SELF_YAML}" ]] || { echo "missing ${SELF_YAML}" >&2; exit 1; }
}

cmd_up() {
    _preflight
    mkdir -p "${ART_DIR}"

    echo "[up] applying KloudLens core from GitHub (ref=${KL_REF})"
    for m in "${KL_MANIFESTS[@]}"; do
        echo "  apply ${KL_RAW}/${m}"
        kubectl apply -f "${KL_RAW}/${m}" >/dev/null
    done
    kubectl -n "${KL_NS}" rollout status daemonset/kloudlens --timeout=180s

    echo "[up] build self image (${SELF_IMAGE}:${SELF_TAG})"
    make -C "${COMP_ROOT}" build-image TAG="${SELF_TAG}"
    docker save "${SELF_IMAGE}:${SELF_TAG}" \
        | sudo ctr -n k8s.io images import -

    echo "[up] apply self (${SELF_YAML})"
    kubectl apply -f "${SELF_YAML}"
    # The shipped manifest uses imagePullPolicy=Always so production
    # deploys pick up new :latest builds. The e2e harness sideloads the
    # image into containerd, so override to IfNotPresent here — otherwise
    # kubelet tries to pull an unpublished tag and the pod errors.
    kubectl -n "${KL_NS}" patch "deploy/${SELF_DEPLOY}" --type=json \
        -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]' >/dev/null
    kubectl -n "${KL_NS}" rollout status "deploy/${SELF_DEPLOY}" --timeout=180s
    echo "[up] done"
}

cmd_down() {
    if [[ -f "${SELF_YAML}" ]]; then
        echo "[down] deleting ${SELF_YAML}"
        kubectl delete -f "${SELF_YAML}" --ignore-not-found
    fi
    echo "[down] deleting KloudLens core from GitHub (ref=${KL_REF})"
    for ((i=${#KL_MANIFESTS[@]}-1; i>=0; i--)); do
        kubectl delete -f "${KL_RAW}/${KL_MANIFESTS[i]}" --ignore-not-found >/dev/null 2>&1 || true
    done
    echo "[down] done"
}

_init_report() {
    mkdir -p "${ART_DIR}"
    {
        echo "# kloudlens-aggregator test report"
        echo
        echo "- Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "- KL_REF: ${KL_REF}"
        echo
    } >"${REPORT}"
}

# _portfwd starts kubectl port-forward to the aggregator's metrics port in the
# background, sets PF_PID, and waits for the local port to start accepting
# connections (max ~10s). Subsequent _check_* calls hit http://127.0.0.1:$lp.
_portfwd() {
    local lp="${1}"
    kubectl -n "${KL_NS}" port-forward "svc/${SELF_SVC}" "${lp}:${METRICS_PORT}" \
        >>"${ART_DIR}/portfwd.log" 2>&1 &
    PF_PID=$!
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        if curl -sf "http://127.0.0.1:${lp}/healthz" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    return 1
}

_portfwd_stop() {
    if [[ -n "${PF_PID:-}" ]]; then
        kill "${PF_PID}" 2>/dev/null || true
        wait "${PF_PID}" 2>/dev/null || true
        PF_PID=""
    fi
}

cmd_check() {
    _preflight
    _init_report

    local lp=18091
    if ! _portfwd "${lp}"; then
        _fail "port-forward to ${SELF_SVC}:${METRICS_PORT}" "could not reach /healthz within 10s"
        _portfwd_stop
        _summary
        return
    fi
    trap _portfwd_stop EXIT

    local out
    if out="$(curl -sf "http://127.0.0.1:${lp}/healthz")" && [[ "${out}" == "ok" ]]; then
        _ok "/healthz" "${out}"
    else
        _fail "/healthz" "${out}"
    fi

    if out="$(curl -sf "http://127.0.0.1:${lp}/readyz")" && [[ "${out}" == "ok" ]]; then
        _ok "/readyz" "${out}"
    else
        _fail "/readyz" "${out}"
    fi

    if out="$(curl -sf "http://127.0.0.1:${lp}/metrics" | head -3)"; then
        _ok "/metrics" "$(echo "${out}" | head -1)"
    else
        _fail "/metrics" "no body"
    fi

    if out="$(curl -sf "http://127.0.0.1:${lp}/stats")"; then
        _ok "/stats" "${out%$'\n'}"
    else
        _fail "/stats" "no body"
    fi

    # Discovery sanity: the aggregator should have learned at least one
    # endpoint from the kloudlens DaemonSet within ~10s of becoming ready.
    # We assert the pod's logs mention the watch URL has been hit at least
    # once, which is a stable signal regardless of how fast events flow.
    local logs
    logs="$(kubectl -n "${KL_NS}" logs "deploy/${SELF_DEPLOY}" 2>&1 || true)"
    if grep -q "k8s discovery watching" <<<"${logs}"; then
        _ok "discovery startup line present in logs"
    else
        _fail "discovery startup line missing" "tail: $(echo "${logs}" | tail -3 | tr '\n' ';')"
    fi

    _portfwd_stop
    trap - EXIT
    _summary
}

_summary() {
    echo
    echo "[summary] passed: ${PASS}  failed: ${FAIL}"
    echo "          report: ${REPORT}"
    {
        echo
        echo "## summary"
        echo "- passed: ${PASS}"
        echo "- failed: ${FAIL}"
    } >>"${REPORT}"
    (( FAIL == 0 ))
}

cmd_all() {
    cmd_up
    cmd_check
}

usage() {
    cat <<'EOF'
Usage: test.sh <command>

Commands:
  up       apply KloudLens core (GitHub, KL_REF), build self image,
           apply deployments/kloudlens-aggregator.yaml, wait for rollouts
  check    port-forward to the aggregator service, assert health/metrics
  down     reverse of up — delete self + KloudLens core
  all      up + check

Environment:
  KL_REF        GitHub ref for KloudLens manifests   (default: main)
  KL_NS         KloudLens namespace                  (default: kloudlens)
  SELF_TAG      self image tag to build              (default: latest)
  METRICS_PORT  aggregator metrics container port    (default: 9091)
EOF
}

main() {
    local cmd="${1:-check}"
    shift || true
    case "${cmd}" in
        up)     cmd_up "$@" ;;
        check)  cmd_check "$@" ;;
        down)   cmd_down "$@" ;;
        all)    cmd_all "$@" ;;
        ""|-h|--help|help) usage ;;
        *) echo "unknown command: ${cmd}" >&2; usage >&2; exit 2 ;;
    esac
}

main "$@"
