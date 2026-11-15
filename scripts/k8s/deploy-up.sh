#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

TAG="${IMAGE_TAG:-$(image_tag)}"
export IMAGE_TAG="$TAG"

"$SCRIPT_DIR/deploy-infra.sh"
"$SCRIPT_DIR/migrate.sh"
"$SCRIPT_DIR/deploy-app.sh"
"$SCRIPT_DIR/deploy-monitoring.sh"
echo "Stack up. IMAGE_TAG=${TAG}"
echo "Port-forward: scripts/k8s/port-forward.sh"