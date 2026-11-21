#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

COMPONENT="${1:-all}"
case "$COMPONENT" in
  controller) kubectl_ns logs deployment/controller -f --tail=100 ;;
  worker) kubectl_ns logs deployment/worker -f --tail=100 --all-containers ;;
  migrate) kubectl_ns logs job/migrate --tail=200 ;;
  postgres) kubectl_ns logs statefulset/postgres -f --tail=100 ;;
  nats) kubectl_ns logs statefulset/nats -f --tail=100 ;;
  all)
    for c in postgres nats controller worker; do
      echo "=== $c ==="
      kubectl_ns logs "deployment/$c" --tail=30 2>/dev/null || kubectl_ns logs "statefulset/$c" --tail=30 2>/dev/null || true
    done
    ;;
  *) echo "usage: logs.sh [controller|worker|migrate|postgres|nats|all]" >&2; exit 1 ;;
esac