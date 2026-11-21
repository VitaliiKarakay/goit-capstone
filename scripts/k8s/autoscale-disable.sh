#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

if ! kubectl_ns get scaledobject worker &>/dev/null; then
  echo "ScaledObject/worker not present; ensuring manual worker scale (replicas=2)"
  if kubectl_ns get deployment worker &>/dev/null; then
    kubectl_ns scale deployment/worker --replicas=2
    kubectl_ns rollout status deployment/worker --timeout="$WAIT_TIMEOUT"
  fi
  exit 0
fi

kubectl_ns delete scaledobject worker --wait=true
wait_worker_keda_hpa_gone
if kubectl_ns get deployment worker &>/dev/null; then
  kubectl_ns scale deployment/worker --replicas=2
  kubectl_ns rollout status deployment/worker --timeout="$WAIT_TIMEOUT"
fi
echo "Worker autoscaling disabled; manual replicas=2"