#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

kubectl_ctx apply -f "$REPO_ROOT/deploy/k8s/base/secret.example.yaml"