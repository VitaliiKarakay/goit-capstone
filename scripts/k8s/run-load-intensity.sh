#!/usr/bin/env bash
# Staggered scheduled_at load intensity (scheduler-local, 2 workers).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

TRIAL=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --trial) TRIAL=1; shift ;;
    -h|--help)
      echo "usage: run-load-intensity.sh [--trial]" >&2
      exit 0
      ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

API_PORT="${APP_LOCAL_PORT:-18081}"
PROM_PORT="${PROM_LOCAL_PORT:-19090}"
export API="${API:-http://127.0.0.1:${API_PORT}}"
export PROM_URL="${PROM_URL:-http://127.0.0.1:${PROM_PORT}}"
export LOAD_REFERENCE_DIR="${LOAD_REFERENCE_DIR:-$REPO_ROOT/perf-workers/run-20261122T160711Z}"
export EXPECTED_IMAGE_TAG="${EXPECTED_IMAGE_TAG:-b700b36}"
export LOAD_MOCK_SEND_DELAY="${LOAD_MOCK_SEND_DELAY:-500ms}"

STATE_JSON="${TMPDIR:-/tmp}/scheduler-load-state-$$.json"
PF_API_PID=""
PF_PROM_PID=""
RESTORED=0

save_initial_state() {
  local mock_delay mock_fail replicas autoscale
  mock_delay="$(kubectl_ns get configmap scheduler-app -o jsonpath='{.data.MOCK_SEND_DELAY}' 2>/dev/null || echo "")"
  mock_fail="$(kubectl_ns get configmap scheduler-app -o jsonpath='{.data.MOCK_FAILURE_RATE}' 2>/dev/null || echo "")"
  replicas="$(kubectl_ns get deployment worker -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "2")"
  autoscale="false"
  if worker_autoscaling_active; then autoscale="true"; fi
  python3 - <<PY >"$STATE_JSON"
import json
print(json.dumps({
  "mock_send_delay": "${mock_delay}",
  "mock_failure_rate": "${mock_fail}",
  "worker_replicas": int("${replicas}" or "2"),
  "autoscale_active": "${autoscale}" == "true",
  "pf_api_started": False,
  "pf_prom_started": False,
}))
PY
}

http_ok() { curl -sf --max-time 5 "$1" >/dev/null 2>&1; }

maybe_start_port_forwards() {
  kubectl_ns rollout status deployment/controller --timeout="${WAIT_TIMEOUT:-180s}" || true
  kubectl_ns rollout status deployment/worker --timeout="${WAIT_TIMEOUT:-180s}" || true
  if ! http_ok "${API}/healthz"; then
    kubectl_ns port-forward --address 127.0.0.1 svc/controller "${API_PORT}:8080" >/dev/null 2>&1 &
    PF_API_PID=$!
    python3 -c "import json; p=json.load(open('$STATE_JSON')); p['pf_api_started']=True; json.dump(p,open('$STATE_JSON','w'))"
    for _ in $(seq 1 60); do http_ok "${API}/healthz" && break; sleep 1; done
    http_ok "${API}/healthz" || { echo "error: API unreachable" >&2; exit 1; }
  fi
  if ! http_ok "${PROM_URL}/-/ready"; then
    kubectl_ns port-forward --address 127.0.0.1 svc/prometheus "${PROM_PORT}:9090" >/dev/null 2>&1 &
    PF_PROM_PID=$!
    python3 -c "import json; p=json.load(open('$STATE_JSON')); p['pf_prom_started']=True; json.dump(p,open('$STATE_JSON','w'))"
    for _ in $(seq 1 60); do http_ok "${PROM_URL}/-/ready" && break; sleep 1; done
    http_ok "${PROM_URL}/-/ready" || { echo "error: Prometheus unreachable" >&2; exit 1; }
  fi
}

restore_stand() {
  [[ "$RESTORED" -eq 1 ]] && return 0
  RESTORED=1
  local mock_delay mock_fail replicas autoscale pf_api pf_prom
  read -r mock_delay mock_fail replicas autoscale pf_api pf_prom < <(python3 - <<PY
import json
p=json.load(open("$STATE_JSON"))
print(p.get("mock_send_delay","0s"), p.get("mock_failure_rate","0"), p.get("worker_replicas",2),
      "true" if p.get("autoscale_active") else "false",
      "true" if p.get("pf_api_started") else "false",
      "true" if p.get("pf_prom_started") else "false")
PY
)
  if kubectl_ns get configmap scheduler-app &>/dev/null; then
    kubectl_ns patch configmap scheduler-app --type merge -p "$(python3 - "$mock_delay" "$mock_fail" <<'PY'
import json,sys; print(json.dumps({"data":{"MOCK_SEND_DELAY":sys.argv[1],"MOCK_FAILURE_RATE":sys.argv[2]}}))
PY
)"
    kubectl_ns rollout restart deployment/worker
    kubectl_ns rollout status deployment/worker --timeout="$WAIT_TIMEOUT" || true
  fi
  if [[ "$autoscale" == "true" ]]; then
    [[ -x "$SCRIPT_DIR/autoscale-enable.sh" ]] && "$SCRIPT_DIR/autoscale-enable.sh" || true
  elif kubectl_ns get deployment worker &>/dev/null; then
    kubectl_ns scale deployment/worker --replicas="$replicas"
    kubectl_ns rollout status deployment/worker --timeout="$WAIT_TIMEOUT" || true
  fi
  [[ "$pf_api" == "true" && -n "$PF_API_PID" ]] && kill "$PF_API_PID" 2>/dev/null || true
  [[ "$pf_prom" == "true" && -n "$PF_PROM_PID" ]] && kill "$PF_PROM_PID" 2>/dev/null || true
  rm -f "$STATE_JSON"
}

trap restore_stand EXIT
save_initial_state

if [[ "$TRIAL" -eq 1 ]]; then
  TRIAL_DIR="$(mktemp -d "${TMPDIR:-/tmp}/load-intensity-trial.XXXXXX")"
  export LOAD_OUTPUT_DIR="$TRIAL_DIR"
  export LOAD_TRIAL=1
  export LOAD_JOB_COUNT=120
  export LOAD_CREATE_LEAD_SECONDS=120
  echo "Trial: $TRIAL_DIR" >&2
else
  export LOAD_OUTPUT_DIR="${LOAD_OUTPUT_DIR:-$REPO_ROOT/perf-load-intensity/run-$(date -u +%Y%m%dT%H%M%SZ)}"
  mkdir -p "$LOAD_OUTPUT_DIR"
  export LOAD_TRIAL=0
  export LOAD_JOB_COUNT=1000
  export LOAD_CREATE_LEAD_SECONDS=180
  echo "Output: $LOAD_OUTPUT_DIR" >&2
fi

maybe_start_port_forwards
if worker_autoscaling_active; then
  "$SCRIPT_DIR/autoscale-disable.sh"
fi

export GIT_SHA="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
RC=0
python3 "$SCRIPT_DIR/verify-load-intensity.py" || RC=$?
if [[ "$TRIAL" -eq 1 && "$RC" -eq 0 && "${LOAD_KEEP_TRIAL:-0}" != "1" ]]; then
  rm -rf "$TRIAL_DIR"
fi
exit "$RC"