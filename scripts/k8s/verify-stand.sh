#!/usr/bin/env bash
# Quick stand health: worker readiness count + Prometheus scheduler-workers UP count.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

EXPECTED="${1:?usage: verify-stand.sh <worker_replicas>}"
PROM_PORT="${PROM_LOCAL_PORT:-19090}"

kubectl_ns wait --for=condition=available "deployment/worker" --timeout="$WAIT_TIMEOUT"
ready="$(kubectl_ns get deployment worker -o jsonpath='{.status.readyReplicas}')"
if [[ "${ready:-0}" != "$EXPECTED" ]]; then
  echo "worker readyReplicas=$ready want $EXPECTED" >&2
  kubectl_ns get pods -l app.kubernetes.io/name=worker
  exit 1
fi

kubectl_ns wait --for=condition=ready pod -l app.kubernetes.io/name=worker --timeout="$WAIT_TIMEOUT" --field-selector=status.phase=Running

kubectl_ns port-forward --address 127.0.0.1 svc/prometheus "$PROM_PORT:9090" >/tmp/k8s-prom-pf.log 2>&1 &
PF=$!
trap 'kill $PF 2>/dev/null' EXIT
sleep 2

PROM_URL="http://127.0.0.1:${PROM_PORT}" "$SCRIPT_DIR/verify-prometheus-targets.sh" "$EXPECTED"
echo "stand ok: $EXPECTED/$EXPECTED worker pods ready, Prometheus targets UP"