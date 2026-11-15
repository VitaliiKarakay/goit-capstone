#!/usr/bin/env bash
# SIGKILL the worker process and confirm restartCount / termination state.
# In-container kill -9 1 via kubectl exec does not always restart on the minikube docker driver;
# we send SIGKILL to the container main PID on the minikube node instead.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

POD="${1:?usage: verify-kill-worker.sh <worker-pod-name>}"
kubectl_ns wait --for=condition=ready "pod/$POD" --timeout=120s
before="$(kubectl_ns get pod "$POD" -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null || echo 0)"
cid="$(kubectl_ns get pod "$POD" -o jsonpath='{.status.containerStatuses[0].containerID}' | sed 's|docker://||')"

if [[ -z "$cid" ]]; then
  echo "container ID not found for pod/$POD" >&2
  exit 1
fi

minikube ssh -p "$MINIKUBE_PROFILE" -- \
  "sudo kill -9 \"\$(sudo docker inspect -f '{{.State.Pid}}' '$cid')\""

kubectl_ns wait --for=condition=ready "pod/$POD" --timeout=120s

after="$(kubectl_ns get pod "$POD" -o jsonpath='{.status.containerStatuses[0].restartCount}')"
reason="$(kubectl_ns get pod "$POD" -o jsonpath='{.status.containerStatuses[0].lastState.terminated.reason}')"
exit_code="$(kubectl_ns get pod "$POD" -o jsonpath='{.status.containerStatuses[0].lastState.terminated.exitCode}')"

if [[ "${after:-0}" -le "${before:-0}" ]]; then
  echo "expected restartCount to increase (before=$before after=$after)" >&2
  kubectl_ns get pod "$POD" -o yaml | tail -50
  exit 1
fi

echo "SIGKILL confirmed: restartCount $before -> $after (lastState.reason=${reason:-n/a}, exitCode=${exit_code:-n/a})"