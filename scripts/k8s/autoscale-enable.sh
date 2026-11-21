#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

if ! kubectl_ctx get crd scaledobjects.keda.sh &>/dev/null; then
  echo "error: KEDA CRDs not found; run scripts/k8s/keda-install.sh" >&2
  exit 1
fi

kubectl_ns apply -f "$REPO_ROOT/deploy/k8s/autoscaling/scaledobject-worker.yaml"
kubectl_ns wait --for=condition=Ready "scaledobject/worker" --timeout=120s 2>/dev/null || true

echo "Worker autoscaling enabled (ScaledObject/worker, min=1 max=4, threshold 50 queued/pod)."
echo "Manual scale: disabled while ScaledObject exists — use scripts/k8s/autoscale-disable.sh"