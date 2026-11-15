#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

BASE="$REPO_ROOT/deploy/k8s/base"
kubectl_ctx apply -f "$BASE/namespace.yaml"
"$SCRIPT_DIR/apply-secrets.sh"
kubectl_ns apply \
  -f "$BASE/configmap-app.yaml" \
  -f "$BASE/postgres.yaml" \
  -f "$BASE/nats.yaml"
wait_sts_ready postgres
wait_sts_ready nats
echo "Infrastructure ready in namespace $KUBE_NAMESPACE"