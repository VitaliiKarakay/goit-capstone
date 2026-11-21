#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

TAG="$(pin_image_tag)"

docker build -f "$REPO_ROOT/Dockerfile" --target controller \
  -t "scheduler/controller:${TAG}" "$REPO_ROOT"
docker build -f "$REPO_ROOT/Dockerfile" --target worker \
  -t "scheduler/worker:${TAG}" "$REPO_ROOT"

echo "IMAGE_TAG=${TAG}"
echo "Pinned tag file: ${IMAGE_TAG_FILE}"
echo "Built scheduler/controller:${TAG} and scheduler/worker:${TAG}"