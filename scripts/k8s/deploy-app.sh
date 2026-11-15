#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

export IMAGE_TAG="$(require_pinned_image_tag)"
kustomize_build_app | kubectl_ctx apply -f -
kubectl_ns scale deployment/controller --replicas=1
kubectl_ns scale deployment/worker --replicas="${WORKER_REPLICAS:-2}"
kubectl_ns rollout status deployment/controller --timeout="$WAIT_TIMEOUT"
kubectl_ns rollout status deployment/worker --timeout="$WAIT_TIMEOUT"
echo "Application workloads running (controller=1, worker=${WORKER_REPLICAS:-2}, IMAGE_TAG=${IMAGE_TAG})"