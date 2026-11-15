#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

"$SCRIPT_DIR/build-images.sh"
"$SCRIPT_DIR/load-images.sh"
export IMAGE_TAG="$(require_pinned_image_tag)"
"$SCRIPT_DIR/migrate.sh"
"$SCRIPT_DIR/deploy-app.sh"
echo "Upgraded application to ${IMAGE_TAG}"