#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

export IMAGE_TAG="$(require_pinned_image_tag)"
apply_app_manifests
kubectl_ns scale deployment/controller --replicas=1
kubectl_ns rollout status deployment/controller --timeout="$WAIT_TIMEOUT"
if worker_autoscaling_active; then
  kubectl_ns rollout status deployment/worker --timeout="$WAIT_TIMEOUT"
  echo "Application workloads running (controller=1, worker autoscaling active, IMAGE_TAG=${IMAGE_TAG})"
else
  kubectl_ns rollout status deployment/worker --timeout="$WAIT_TIMEOUT"
  echo "Application workloads running (controller=1, worker=${WORKER_REPLICAS:-2}, IMAGE_TAG=${IMAGE_TAG})"
fi