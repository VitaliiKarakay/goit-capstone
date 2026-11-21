#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

KEDA_NAMESPACE="${KEDA_NAMESPACE:-keda}"

if kubectl_ctx get namespace "$KEDA_NAMESPACE" &>/dev/null; then
  helm uninstall keda \
    --kube-context "$KUBE_CONTEXT" \
    --namespace "$KEDA_NAMESPACE" \
    || true
else
  echo "namespace ${KEDA_NAMESPACE} not found; nothing to uninstall"
fi

echo "KEDA removed from context ${KUBE_CONTEXT}"