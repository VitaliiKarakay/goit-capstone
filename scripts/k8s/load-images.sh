#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

TAG="$(require_pinned_image_tag)"
export IMAGE_TAG="$TAG"
minikube -p "$MINIKUBE_PROFILE" image load "scheduler/controller:${TAG}"
minikube -p "$MINIKUBE_PROFILE" image load "scheduler/worker:${TAG}"
echo "Loaded images into profile $MINIKUBE_PROFILE (IMAGE_TAG=${TAG})"