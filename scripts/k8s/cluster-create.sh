#!/usr/bin/env bash
set -euo pipefail

: "${KUBE_CONTEXT:=scheduler-local}"
: "${MINIKUBE_PROFILE:=scheduler-local}"

if [[ "$KUBE_CONTEXT" != "scheduler-local" ]]; then
  echo "error: KUBE_CONTEXT must be scheduler-local (got: $KUBE_CONTEXT)" >&2
  exit 1
fi

K8S_VERSION="${K8S_VERSION:-v1.31.4}"
CPUS="${MINIKUBE_CPUS:-4}"
MEMORY="${MINIKUBE_MEMORY:-6144}"

if command -v docker &>/dev/null; then
  docker info &>/dev/null || echo "warning: docker may not be running; minikube needs Docker Desktop resources >= ${CPUS} CPUs and ${MEMORY} MiB" >&2
fi

if minikube status -p "$MINIKUBE_PROFILE" 2>/dev/null | grep -qiE 'host: Running|kubelet: Running'; then
  echo "minikube profile $MINIKUBE_PROFILE already running"
  exit 0
fi

minikube start -p "$MINIKUBE_PROFILE" \
  --driver=docker \
  --kubernetes-version="$K8S_VERSION" \
  --cpus="$CPUS" \
  --memory="$MEMORY" \
  --keep-context

if ! kubectl config get-contexts "$KUBE_CONTEXT" &>/dev/null; then
  echo "error: expected kubectl context $KUBE_CONTEXT after minikube start" >&2
  exit 1
fi

echo "Cluster $MINIKUBE_PROFILE ready (Kubernetes $K8S_VERSION). Current kubectl context unchanged (--keep-context)."
echo "Use: kubectl --context $KUBE_CONTEXT -n scheduler …"