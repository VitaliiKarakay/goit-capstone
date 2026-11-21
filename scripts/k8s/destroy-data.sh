#!/usr/bin/env bash
# Deletes scheduler stand PVCs only (irreversible). Requires workloads scaled down.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

"$SCRIPT_DIR/stop.sh"
kubectl_ns scale statefulset/postgres --replicas=0
kubectl_ns scale statefulset/nats --replicas=0

if ! kubectl_ns wait --for=delete pod -l app.kubernetes.io/name=postgres --timeout=300s; then
  echo "error: postgres pods still present; refusing to delete PVCs" >&2
  kubectl_ns get pods -l app.kubernetes.io/name=postgres || true
  exit 1
fi
if ! kubectl_ns wait --for=delete pod -l app.kubernetes.io/name=nats --timeout=300s; then
  echo "error: nats pods still present; refusing to delete PVCs" >&2
  kubectl_ns get pods -l app.kubernetes.io/name=nats || true
  exit 1
fi

kubectl_ns delete pvc -l app.kubernetes.io/name=postgres --ignore-not-found
kubectl_ns delete pvc -l app.kubernetes.io/name=nats --ignore-not-found

echo "PVCs deleted in $KUBE_NAMESPACE (postgres/nats only). StatefulSets remain; run deploy-up.sh to recreate pods with empty data."