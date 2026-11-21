#!/usr/bin/env bash
# Expects prometheus port-forward on 127.0.0.1:9090 (or PROM_LOCAL_PORT).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

EXPECTED="${1:?usage: verify-prometheus-targets.sh <expected_worker_up_count>}"
PROM="${PROM_URL:-http://127.0.0.1:${PROM_LOCAL_PORT:-9090}}"

json="$(curl -sf "${PROM}/api/v1/targets")"
up="$(echo "$json" | python3 -c "
import json,sys
d=json.load(sys.stdin)
t=[x for x in d['data']['activeTargets']
   if x.get('labels',{}).get('job')=='scheduler-workers' and x.get('health')=='up']
pods=sorted({x['labels'].get('pod','') for x in t})
dup=len(pods)!=len(t)
print(len(t))
if dup:
    print('duplicate pod targets', file=sys.stderr)
    sys.exit(2)
")"

if [[ "$up" != "$EXPECTED" ]]; then
  echo "expected $EXPECTED healthy scheduler-workers targets, got $up" >&2
  echo "$json" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for x in d['data']['activeTargets']:
    if x.get('labels',{}).get('job')=='scheduler-workers':
        print(x.get('health'), x.get('labels',{}).get('pod'), x.get('scrapeUrl'))
" >&2
  exit 1
fi
echo "scheduler-workers: $up target(s) up (expected $EXPECTED), no duplicate pod entries"