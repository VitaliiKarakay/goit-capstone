#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

export IMAGE_TAG="$(require_pinned_image_tag)"
kustomize_build_monitoring | kubectl_ctx apply -f -
kubectl_ns scale deployment/prometheus --replicas=1
kubectl_ns scale deployment/grafana --replicas=1
kubectl_ns rollout status deployment/prometheus --timeout="$WAIT_TIMEOUT"
kubectl_ns rollout status deployment/grafana --timeout="$WAIT_TIMEOUT"
echo "Monitoring running (does not change app deployments or worker scale)"