#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

TAG="$(require_pinned_image_tag)"
export IMAGE_TAG="$TAG"
scale_app_workloads_to_zero

kubectl_ns delete job migrate --ignore-not-found=true
kubectl_ns wait --for=delete job/migrate --timeout=120s 2>/dev/null || true

sed "s|scheduler/controller:unset|scheduler/controller:${TAG}|g" \
  "$REPO_ROOT/deploy/k8s/base/job-migrate.yaml" | kubectl_ns apply -f -

if ! wait_job_complete migrate; then
  echo "migration failed; not starting application workloads" >&2
  kubectl_ns logs "job/migrate" --tail=100 || true
  exit 1
fi
echo "Migrations complete (job/migrate)"