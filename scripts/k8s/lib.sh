#!/usr/bin/env bash
# Shared helpers for local minikube (scheduler-local only).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export REPO_ROOT

: "${KUBE_CONTEXT:=scheduler-local}"
: "${KUBE_NAMESPACE:=scheduler}"
: "${MINIKUBE_PROFILE:=scheduler-local}"
: "${WAIT_TIMEOUT:=300s}"

if [[ "$KUBE_CONTEXT" != "scheduler-local" ]]; then
  echo "error: KUBE_CONTEXT must be scheduler-local (got: $KUBE_CONTEXT)" >&2
  echo "Refusing to touch another cluster (e.g. EKS)." >&2
  exit 1
fi

# Local client bundled with repo (v1.31.x) when present; never changes default kubeconfig / EKS context.
K8S_KUBECTL_BIN="${K8S_KUBECTL_BIN:-}"
if [[ -z "$K8S_KUBECTL_BIN" && -x "$REPO_ROOT/scripts/k8s/bin/kubectl" ]]; then
  K8S_KUBECTL_BIN="$REPO_ROOT/scripts/k8s/bin/kubectl"
fi
if [[ -z "$K8S_KUBECTL_BIN" ]]; then
  K8S_KUBECTL_BIN="$(command -v kubectl)"
fi

if ! "$K8S_KUBECTL_BIN" config get-contexts "$KUBE_CONTEXT" &>/dev/null; then
  echo "error: kubectl context $KUBE_CONTEXT not found; run scripts/k8s/cluster-create.sh" >&2
  exit 1
fi

kubectl_ctx() {
  "$K8S_KUBECTL_BIN" --context "$KUBE_CONTEXT" "$@"
}

kubectl_ns() {
  "$K8S_KUBECTL_BIN" --context "$KUBE_CONTEXT" -n "$KUBE_NAMESPACE" "$@"
}

image_tag() {
  local sha
  sha="$(git -C "$REPO_ROOT" rev-parse --short HEAD)"
  if git -C "$REPO_ROOT" diff --quiet && git -C "$REPO_ROOT" diff --cached --quiet; then
    echo "$sha"
  else
    echo "${sha}-dirty-$(date -u +%Y%m%d%H%M%S)"
  fi
}

wait_rollout_gone() {
  local deploy="$1"
  local timeout="${2:-$WAIT_TIMEOUT}"
  if ! kubectl_ns get deployment "$deploy" &>/dev/null; then
    return 0
  fi
  kubectl_ns rollout status "deployment/$deploy" --timeout="$timeout"
  # Only Running pods block scale-down (Succeeded/Failed terminal pods from jobs are ignored).
  local pods
  pods="$(kubectl_ns get pods -l "app.kubernetes.io/name=$deploy" --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')"
  if [[ "${pods:-0}" != "0" ]]; then
    echo "timeout: deployment/$deploy scaled down but ${pods} running pod(s) still present" >&2
    kubectl_ns get pods -l "app.kubernetes.io/name=$deploy" || true
    return 1
  fi
}

wait_job_complete() {
  local name="$1"
  kubectl_ns wait --for=condition=complete "job/$name" --timeout="$WAIT_TIMEOUT"
}

wait_sts_ready() {
  local name="$1"
  kubectl_ns rollout status "statefulset/$name" --timeout="$WAIT_TIMEOUT"
}

kustomize_build() {
  local tag="${IMAGE_TAG:-$(image_tag)}"
  kubectl_ctx kustomize --load-restrictor=LoadRestrictionsNone \
    "$REPO_ROOT/deploy/k8s/overlays/local" | \
    sed "s|scheduler/controller:local-dev|scheduler/controller:${tag}|g; s|scheduler/worker:local-dev|scheduler/worker:${tag}|g"
}

apply_overlay() {
  kustomize_build | kubectl_ctx apply -f -
}

scale_app_to_zero() {
  for d in controller worker prometheus grafana; do
    if kubectl_ns get deployment "$d" &>/dev/null; then
      kubectl_ns scale "deployment/$d" --replicas=0
    fi
  done
  wait_rollout_gone controller
  wait_rollout_gone worker
  wait_rollout_gone prometheus
  wait_rollout_gone grafana
}