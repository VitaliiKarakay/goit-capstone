#!/usr/bin/env bash
# Destroys the entire local minikube profile and all its data (PVCs included).
set -euo pipefail

: "${KUBE_CONTEXT:=scheduler-local}"
: "${MINIKUBE_PROFILE:=scheduler-local}"

if [[ "$KUBE_CONTEXT" != "scheduler-local" ]]; then
  echo "error: KUBE_CONTEXT must be scheduler-local (got: $KUBE_CONTEXT)" >&2
  exit 1
fi

minikube delete -p "$MINIKUBE_PROFILE"
echo "Deleted minikube profile $MINIKUBE_PROFILE and all local cluster data."