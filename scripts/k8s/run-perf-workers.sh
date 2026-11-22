#!/usr/bin/env bash
# Fixed-replica worker performance series (scheduler-local only).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

TRIAL=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --trial) TRIAL=1; shift ;;
    -h|--help)
      echo "usage: run-perf-workers.sh [--trial]" >&2
      echo "  --trial  short smoke test (not recorded in perf-workers/)" >&2
      exit 0
      ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

API_PORT="${APP_LOCAL_PORT:-18081}"
PROM_PORT="${PROM_LOCAL_PORT:-19090}"
export API="${API:-http://127.0.0.1:${API_PORT}}"
export PROM_URL="${PROM_URL:-http://127.0.0.1:${PROM_PORT}}"

STATE_JSON="${TMPDIR:-/tmp}/scheduler-perf-state-$$.json"
PF_API_PID=""
PF_PROM_PID=""
RESTORED=0

save_initial_state() {
  local mock_delay mock_fail replicas autoscale controller_image worker_image
  mock_delay="$(kubectl_ns get configmap scheduler-app -o jsonpath='{.data.MOCK_SEND_DELAY}' 2>/dev/null || echo "")"
  mock_fail="$(kubectl_ns get configmap scheduler-app -o jsonpath='{.data.MOCK_FAILURE_RATE}' 2>/dev/null || echo "")"
  replicas="$(kubectl_ns get deployment worker -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "2")"
  autoscale="false"
  if worker_autoscaling_active; then autoscale="true"; fi
  controller_image="$(kubectl_ns get deployment controller -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || echo "")"
  worker_image="$(kubectl_ns get deployment worker -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || echo "")"
  python3 - <<PY >"$STATE_JSON"
import json, os
print(json.dumps({
  "mock_send_delay": "${mock_delay}",
  "mock_failure_rate": "${mock_fail}",
  "worker_replicas": int("${replicas}" or "2"),
  "autoscale_active": "${autoscale}" == "true",
  "controller_image_saved": "${controller_image}",
  "worker_image_saved": "${worker_image}",
  "pf_api_started": False,
  "pf_prom_started": False,
  "repo_root": "${REPO_ROOT}",
}))
PY
}

http_ok() {
  curl -sf --max-time 5 "$1" >/dev/null 2>&1
}

maybe_start_port_forwards() {
  if http_ok "${API}/healthz"; then
  :
  else
    kubectl_ns port-forward --address 127.0.0.1 svc/controller "${API_PORT}:8080" \
      >/dev/null 2>&1 &
    PF_API_PID=$!
    python3 - <<PY
import json
p=json.load(open("$STATE_JSON"))
p["pf_api_started"]=True
json.dump(p, open("$STATE_JSON","w"))
PY
    for _ in $(seq 1 30); do
      http_ok "${API}/healthz" && break
      sleep 1
    done
    http_ok "${API}/healthz" || { echo "error: API unreachable at $API" >&2; exit 1; }
  fi
  if http_ok "${PROM_URL}/-/ready"; then
  :
  else
    kubectl_ns port-forward --address 127.0.0.1 svc/prometheus "${PROM_PORT}:9090" \
      >/dev/null 2>&1 &
    PF_PROM_PID=$!
    python3 - <<PY
import json
p=json.load(open("$STATE_JSON"))
p["pf_prom_started"]=True
json.dump(p, open("$STATE_JSON","w"))
PY
    for _ in $(seq 1 30); do
      http_ok "${PROM_URL}/-/ready" && break
      sleep 1
    done
    http_ok "${PROM_URL}/-/ready" || { echo "error: Prometheus unreachable at $PROM_URL" >&2; exit 1; }
  fi
}

restore_stand() {
  [[ "$RESTORED" -eq 1 ]] && return 0
  RESTORED=1
  local mock_delay mock_fail replicas autoscale pf_api pf_prom
  read -r mock_delay mock_fail replicas autoscale pf_api pf_prom < <(
    python3 - <<PY
import json
p=json.load(open("$STATE_JSON"))
print(
    p.get("mock_send_delay", "0s"),
    p.get("mock_failure_rate", "0"),
    p.get("worker_replicas", 2),
    "true" if p.get("autoscale_active") else "false",
    "true" if p.get("pf_api_started") else "false",
    "true" if p.get("pf_prom_started") else "false",
)
PY
  )
  if kubectl_ns get configmap scheduler-app &>/dev/null; then
    kubectl_ns patch configmap scheduler-app --type merge -p "$(python3 - "$mock_delay" "$mock_fail" <<'PY'
import json, sys
print(json.dumps({"data": {"MOCK_SEND_DELAY": sys.argv[1], "MOCK_FAILURE_RATE": sys.argv[2]}}))
PY
)"
    kubectl_ns rollout restart deployment/worker
    kubectl_ns rollout status deployment/worker --timeout="$WAIT_TIMEOUT" || true
  fi
  if [[ "$autoscale" == "true" ]]; then
    if [[ -x "$SCRIPT_DIR/autoscale-enable.sh" ]]; then
      "$SCRIPT_DIR/autoscale-enable.sh" || true
    fi
  else
    if kubectl_ns get deployment worker &>/dev/null; then
      kubectl_ns scale deployment/worker --replicas="$replicas"
      kubectl_ns rollout status deployment/worker --timeout="$WAIT_TIMEOUT" || true
    fi
  fi
  if [[ "$pf_api" == "true" && -n "$PF_API_PID" ]]; then
    kill "$PF_API_PID" 2>/dev/null || true
  fi
  if [[ "$pf_prom" == "true" && -n "$PF_PROM_PID" ]]; then
    kill "$PF_PROM_PID" 2>/dev/null || true
  fi
  rm -f "$STATE_JSON"
}

trap restore_stand EXIT

save_initial_state

if [[ "$TRIAL" -eq 1 ]]; then
  TRIAL_DIR="$(mktemp -d "${TMPDIR:-/tmp}/perf-workers-trial.XXXXXX")"
  export PERF_OUTPUT_DIR="$TRIAL_DIR"
  export PERF_TRIAL=1
  echo "Trial run (artifacts in $TRIAL_DIR, discarded after success)" >&2
else
  TAG="$(date -u +%Y%m%dT%H%M%SZ)"
  export PERF_OUTPUT_DIR="${PERF_OUTPUT_DIR:-$REPO_ROOT/perf-workers/run-$TAG}"
  mkdir -p "$PERF_OUTPUT_DIR"
  export PERF_TRIAL=0
  echo "Output directory: $PERF_OUTPUT_DIR" >&2
fi

maybe_start_port_forwards

if worker_autoscaling_active; then
  "$SCRIPT_DIR/autoscale-disable.sh"
fi

export PERF_STATE_FILE="$STATE_JSON"
export GIT_SHA="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
export IMAGE_TAG_FILE="${IMAGE_TAG_FILE:-$SCRIPT_DIR/.image-tag}"
RC=0
if ! python3 "$SCRIPT_DIR/verify-perf-workers.py"; then
  RC=1
fi

if [[ "$TRIAL" -eq 1 && "$RC" -eq 0 && "${PERF_KEEP_TRIAL:-0}" != "1" ]]; then
  rm -rf "$TRIAL_DIR"
  echo "Trial passed; artifacts removed." >&2
fi

exit "$RC"