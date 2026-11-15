#!/usr/bin/env bash
# Shared helpers for local minikube (scheduler-local only).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export REPO_ROOT

IMAGE_TAG_FILE="${IMAGE_TAG_FILE:-$REPO_ROOT/scripts/k8s/.image-tag}"

: "${KUBE_CONTEXT:=scheduler-local}"
: "${KUBE_NAMESPACE:=scheduler}"
: "${MINIKUBE_PROFILE:=scheduler-local}"
: "${WAIT_TIMEOUT:=300s}"

assert_scheduler_stand_ids() {
  if [[ "$KUBE_CONTEXT" != "scheduler-local" ]]; then
    echo "error: KUBE_CONTEXT must be scheduler-local (got: $KUBE_CONTEXT)" >&2
    echo "Refusing to touch another cluster (e.g. EKS)." >&2
    exit 1
  fi
  if [[ "$MINIKUBE_PROFILE" != "scheduler-local" ]]; then
    echo "error: MINIKUBE_PROFILE must be scheduler-local (got: $MINIKUBE_PROFILE)" >&2
    exit 1
  fi
  if [[ "$KUBE_NAMESPACE" != "scheduler" ]]; then
    echo "error: KUBE_NAMESPACE must be scheduler (got: $KUBE_NAMESPACE)" >&2
    exit 1
  fi
}

assert_scheduler_stand_ids

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

compute_image_tag() {
  local sha
  sha="$(git -C "$REPO_ROOT" rev-parse --short HEAD)"
  if git -C "$REPO_ROOT" diff --quiet && git -C "$REPO_ROOT" diff --cached --quiet; then
    echo "$sha"
  else
    echo "${sha}-dirty-$(date -u +%Y%m%d%H%M%S)"
  fi
}

# Back-compat name
image_tag() {
  compute_image_tag
}

pin_image_tag() {
  local tag="${1:-${IMAGE_TAG:-}}"
  if [[ -z "$tag" ]]; then
    tag="$(compute_image_tag)"
  fi
  printf '%s\n' "$tag" >"$IMAGE_TAG_FILE"
  export IMAGE_TAG="$tag"
  echo "$tag"
}

# Single tag for build → load → deploy (file written by build-images.sh).
current_image_tag() {
  if [[ -n "${IMAGE_TAG:-}" ]]; then
    echo "$IMAGE_TAG"
    return 0
  fi
  if [[ -f "$IMAGE_TAG_FILE" ]]; then
    cat "$IMAGE_TAG_FILE"
    return 0
  fi
  compute_image_tag
}

require_pinned_image_tag() {
  if [[ -n "${IMAGE_TAG:-}" ]]; then
    echo "$IMAGE_TAG"
    return 0
  fi
  if [[ -f "$IMAGE_TAG_FILE" ]]; then
    cat "$IMAGE_TAG_FILE"
    return 0
  fi
  echo "error: no pinned image tag; run scripts/k8s/build-images.sh first or set IMAGE_TAG" >&2
  exit 1
}

_kustomize_with_tag() {
  local overlay_dir="$1"
  local tag="$2"
  kubectl_ctx kustomize --load-restrictor=LoadRestrictionsNone "$overlay_dir" | \
    sed "s|scheduler/controller:local-dev|scheduler/controller:${tag}|g; s|scheduler/worker:local-dev|scheduler/worker:${tag}|g"
}

kustomize_build_app() {
  local tag
  tag="$(require_pinned_image_tag)"
  _kustomize_with_tag "$REPO_ROOT/deploy/k8s/overlays/local-app" "$tag"
}

kustomize_build_monitoring() {
  kubectl_ctx kustomize --load-restrictor=LoadRestrictionsNone \
    "$REPO_ROOT/deploy/k8s/overlays/local-monitoring"
}

wait_labeled_pods_gone() {
  local app_name="$1"
  local timeout="${2:-$WAIT_TIMEOUT}"
  if ! kubectl_ns get deployment "$app_name" &>/dev/null; then
    return 0
  fi
  if ! kubectl_ns get pods -l "app.kubernetes.io/name=${app_name}" --no-headers 2>/dev/null | grep -q .; then
    return 0
  fi
  if ! kubectl_ns wait --for=delete pod -l "app.kubernetes.io/name=${app_name}" --timeout="$timeout"; then
    echo "timeout waiting for ${app_name} pods to terminate/delete" >&2
    kubectl_ns get pods -l "app.kubernetes.io/name=${app_name}" -o wide || true
    return 1
  fi
}

wait_app_workloads_stopped() {
  wait_labeled_pods_gone controller
  wait_labeled_pods_gone worker
}

wait_job_complete() {
  local name="$1"
  kubectl_ns wait --for=condition=complete "job/$name" --timeout="$WAIT_TIMEOUT"
}

wait_sts_ready() {
  local name="$1"
  kubectl_ns rollout status "statefulset/$name" --timeout="$WAIT_TIMEOUT"
}

scale_app_to_zero() {
  for d in controller worker prometheus grafana; do
    if kubectl_ns get deployment "$d" &>/dev/null; then
      kubectl_ns scale "deployment/$d" --replicas=0
    fi
  done
  wait_labeled_pods_gone controller
  wait_labeled_pods_gone worker
  wait_labeled_pods_gone prometheus
  wait_labeled_pods_gone grafana
}

scale_app_workloads_to_zero() {
  for d in controller worker; do
    if kubectl_ns get deployment "$d" &>/dev/null; then
      kubectl_ns scale "deployment/$d" --replicas=0
    fi
  done
  wait_app_workloads_stopped
}