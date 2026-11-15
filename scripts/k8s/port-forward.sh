#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

APP_PORT="${APP_LOCAL_PORT:-18080}"
GRAFANA_PORT="${GRAFANA_LOCAL_PORT:-3000}"
PROM_PORT="${PROM_LOCAL_PORT:-9090}"

if lsof -nP -iTCP:"$APP_PORT" -sTCP:LISTEN &>/dev/null; then
  echo "error: port $APP_PORT already in use; set APP_LOCAL_PORT" >&2
  exit 1
fi
if lsof -nP -iTCP:"$GRAFANA_PORT" -sTCP:LISTEN &>/dev/null; then
  echo "error: port $GRAFANA_PORT already in use; set GRAFANA_LOCAL_PORT" >&2
  exit 1
fi

echo "UI       http://127.0.0.1:${APP_PORT}/ui/"
echo "Grafana  http://127.0.0.1:${GRAFANA_PORT}/ (admin/admin demo)"
kubectl --context "$KUBE_CONTEXT" -n "$KUBE_NAMESPACE" \
  port-forward --address 127.0.0.1 "svc/controller" "${APP_PORT}:8080" &
PID1=$!
kubectl --context "$KUBE_CONTEXT" -n "$KUBE_NAMESPACE" \
  port-forward --address 127.0.0.1 "svc/grafana" "${GRAFANA_PORT}:3000" &
PID2=$!
kubectl --context "$KUBE_CONTEXT" -n "$KUBE_NAMESPACE" \
  port-forward --address 127.0.0.1 "svc/prometheus" "${PROM_PORT}:9090" &
PID3=$!
trap 'kill $PID1 $PID2 $PID3 2>/dev/null' EXIT
wait