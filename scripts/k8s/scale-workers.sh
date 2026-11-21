#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

REPLICAS="${1:?usage: scale-workers.sh <replicas>}"
assert_manual_worker_scale_allowed
kubectl_ns scale deployment/worker --replicas="$REPLICAS"
kubectl_ns rollout status deployment/worker --timeout="$WAIT_TIMEOUT"
echo "worker replicas=$REPLICAS"