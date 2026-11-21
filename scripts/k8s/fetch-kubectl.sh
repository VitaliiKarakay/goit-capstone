#!/usr/bin/env bash
# Downloads kubectl v1.31.4 for this repo only (does not change system kubectl or EKS kubeconfig).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VER="${K8S_VERSION:-v1.31.4}"
OS=linux
ARCH=amd64
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) OS=darwin; ARCH=arm64 ;;
  Darwin-x86_64) OS=darwin; ARCH=amd64 ;;
  Linux-x86_64) OS=linux; ARCH=amd64 ;;
  Linux-aarch64) OS=linux; ARCH=arm64 ;;
esac
URL="https://dl.k8s.io/release/${VER}/bin/${OS}/${ARCH}/kubectl"
mkdir -p "$SCRIPT_DIR/bin"
curl -fsSL -o "$SCRIPT_DIR/bin/kubectl" "$URL"
chmod +x "$SCRIPT_DIR/bin/kubectl"
"$SCRIPT_DIR/bin/kubectl" version --client=true