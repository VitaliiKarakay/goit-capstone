#!/usr/bin/env bash
# SIGKILL the worker process and confirm restartCount / termination state.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

POD="${1:?usage: verify-kill-worker.sh <worker-pod-name>}"
app="$(kubectl_ns get pod "$POD" -o jsonpath='{.metadata.labels.app\.kubernetes\.io/name}' 2>/dev/null || true)"
if [[ "$app" != worker ]]; then
  echo "error: pod/$POD is not a worker (app.kubernetes.io/name=${app:-missing})" >&2
  exit 1
fi

kubectl_ns wait --for=condition=ready "pod/$POD" --timeout=120s
before="$(kubectl_ns get pod "$POD" -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null || echo 0)"
cid="$(kubectl_ns get pod "$POD" -o jsonpath='{.status.containerStatuses[0].containerID}' | sed 's|docker://||')"

if [[ -z "$cid" ]]; then
  echo "container ID not found for pod/$POD" >&2
  exit 1
fi

host_pid="$(minikube ssh -p "$MINIKUBE_PROFILE" -- "sudo docker inspect -f '{{.State.Pid}}' '$cid'" 2>/dev/null | tr -d '\r\n' || true)"
if [[ ! "$host_pid" =~ ^[1-9][0-9]*$ ]]; then
  echo "error: could not resolve container PID on minikube node (got: ${host_pid:-empty})" >&2
  exit 1
fi

minikube ssh -p "$MINIKUBE_PROFILE" -- "sudo kill -9 '$host_pid'"

deadline=$((SECONDS + 120))
after="$before"
while (( SECONDS < deadline )); do
  after="$(kubectl_ns get pod "$POD" -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null || echo "$before")"
  if [[ "${after:-0}" -gt "${before:-0}" ]]; then
    break
  fi
  sleep 1
done

if [[ "${after:-0}" -le "${before:-0}" ]]; then
  echo "expected restartCount to increase after SIGKILL (before=$before after=$after)" >&2
  kubectl_ns get pod "$POD" -o yaml | tail -50
  exit 1
fi

reason="$(kubectl_ns get pod "$POD" -o jsonpath='{.status.containerStatuses[0].lastState.terminated.reason}')"
exit_code="$(kubectl_ns get pod "$POD" -o jsonpath='{.status.containerStatuses[0].lastState.terminated.exitCode}')"
if [[ -z "$reason" && -z "$exit_code" ]]; then
  echo "warning: lastState.terminated not populated yet; waiting for pod status..." >&2
fi

kubectl_ns wait --for=condition=ready "pod/$POD" --timeout=120s

reason="$(kubectl_ns get pod "$POD" -o jsonpath='{.status.containerStatuses[0].lastState.terminated.reason}')"
exit_code="$(kubectl_ns get pod "$POD" -o jsonpath='{.status.containerStatuses[0].lastState.terminated.exitCode}')"
after="$(kubectl_ns get pod "$POD" -o jsonpath='{.status.containerStatuses[0].restartCount}')"

echo "SIGKILL confirmed: restartCount $before -> $after (lastState.reason=${reason:-n/a}, exitCode=${exit_code:-n/a})"