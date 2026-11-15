#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

TAG="${IMAGE_TAG:-$(image_tag)}"
export IMAGE_TAG="$TAG"

docker build -f "$REPO_ROOT/Dockerfile" --target controller \
  -t "scheduler/controller:${TAG}" "$REPO_ROOT"
docker build -f "$REPO_ROOT/Dockerfile" --target worker \
  -t "scheduler/worker:${TAG}" "$REPO_ROOT"

echo "IMAGE_TAG=${TAG}"
echo "Built scheduler/controller:${TAG} and scheduler/worker:${TAG}"