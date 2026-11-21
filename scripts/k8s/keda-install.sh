#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

KEDA_CHART_VERSION="${KEDA_CHART_VERSION:-2.18.3}"
KEDA_NAMESPACE="${KEDA_NAMESPACE:-keda}"

helm repo add kedacore https://kedacore.github.io/charts 2>/dev/null || true
helm repo update kedacore

helm upgrade --install keda kedacore/keda \
  --kube-context "$KUBE_CONTEXT" \
  --namespace "$KEDA_NAMESPACE" \
  --create-namespace \
  --version "$KEDA_CHART_VERSION" \
  --wait \
  --timeout 10m

echo "KEDA chart ${KEDA_CHART_VERSION} installed in namespace ${KEDA_NAMESPACE} (context ${KUBE_CONTEXT})"