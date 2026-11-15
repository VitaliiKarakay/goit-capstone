#!/usr/bin/env bash
# Stops application and monitoring workloads; keeps namespace, PVCs, and StatefulSets.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

scale_app_to_zero
echo "Workloads scaled to 0. Postgres/NATS PVCs and namespace $KUBE_NAMESPACE are preserved."
echo "To remove data only: scripts/k8s/destroy-data.sh"
echo "To destroy entire cluster: scripts/k8s/cluster-delete.sh"