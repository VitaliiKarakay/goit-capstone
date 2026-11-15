#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

TAG="${IMAGE_TAG:-$(image_tag)}"
export IMAGE_TAG="$TAG"
"$SCRIPT_DIR/build-images.sh"
"$SCRIPT_DIR/load-images.sh"
"$SCRIPT_DIR/migrate.sh"
"$SCRIPT_DIR/deploy-app.sh"
echo "Upgraded application to ${TAG}"