#!/usr/bin/env bash
# Deletes PVCs in namespace scheduler (irreversible). Requires workloads scaled down.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

"$SCRIPT_DIR/stop.sh"
kubectl_ns scale statefulset/postgres --replicas=0
kubectl_ns scale statefulset/nats --replicas=0
kubectl_ns wait --for=delete pod -l app.kubernetes.io/name=postgres --timeout=300s 2>/dev/null || true
kubectl_ns wait --for=delete pod -l app.kubernetes.io/name=nats --timeout=300s 2>/dev/null || true

kubectl_ns delete pvc -l app.kubernetes.io/name=postgres --ignore-not-found
kubectl_ns delete pvc -l app.kubernetes.io/name=nats --ignore-not-found
kubectl_ns delete pvc --all --ignore-not-found

echo "PVCs deleted in $KUBE_NAMESPACE. StatefulSets remain; run deploy-up.sh to recreate pods with empty data."