#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

export IMAGE_TAG="$(require_pinned_image_tag)"

"$SCRIPT_DIR/deploy-infra.sh"
"$SCRIPT_DIR/migrate.sh"
"$SCRIPT_DIR/deploy-app.sh"
"$SCRIPT_DIR/deploy-monitoring.sh"
echo "Stack up. IMAGE_TAG=${IMAGE_TAG}"
echo "Port-forward: scripts/k8s/port-forward.sh"