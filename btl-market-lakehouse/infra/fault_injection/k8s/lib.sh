# Helpers for the Kubernetes fault-injection scripts. Sourced, not executed.
# shellcheck shell=bash
set -euo pipefail

command -v kubectl >/dev/null 2>&1 || { echo "kubectl not found" >&2; exit 1; }

# Wall-clock seconds since the fault was injected; printed so the operator can copy it
# into docs/lessons/D-fault-injection.md ("thời gian phục hồi").
FAULT_STARTED_AT="$(date +%s)"
elapsed() { echo $(( $(date +%s) - FAULT_STARTED_AT )); }

stamp() { date -u +%Y-%m-%dT%H:%M:%SZ; }

banner() {
  printf '\n\033[1;35m[%s] %s\033[0m\n' "$(stamp)" "$*"
}

observe() {
  printf '\033[1;36mQuan sát trên Grafana:\033[0m %s\n' "$*"
}

record() {
  printf '\033[1;33mGhi vào docs/lessons/D-fault-injection.md:\033[0m %s\n' "$*"
}
