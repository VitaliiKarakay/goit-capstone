#!/usr/bin/env bash
# Removes scheduler stand objects that were accidentally created in the default namespace
# (first deploy-infra without -n). Does not touch namespace/default itself or other workloads.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

echo "Dry-run: resources that would be deleted in default (scheduler-local only):"
kubectl_ctx get pvc -n default -o name 2>/dev/null | rg 'data-(nats|postgres)-0' || true
kubectl_ctx get sts,svc,cm -n default -l 'app.kubernetes.io/part-of=goit-capstone' -o name 2>/dev/null || true

if [[ "${1:-}" != "--apply" ]]; then
  echo "Re-run with --apply to delete only the PVCs below (orphan data volumes from mistaken apply)."
  echo "  kubectl --context scheduler-local delete pvc -n default data-postgres-0 data-nats-0"
  exit 0
fi

kubectl_ctx delete pvc -n default data-postgres-0 data-nats-0 --ignore-not-found
echo "Deleted orphan PVCs in default (if they existed)."