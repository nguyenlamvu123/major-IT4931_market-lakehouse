# Shared helpers for the make up/down/images/secrets scripts. Sourced, not executed.
# shellcheck shell=bash

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
K8S="${ROOT}/infra/k8s"
HELM_DIR="${K8S}/helm"
MINIKUBE_PROFILE="${MINIKUBE_PROFILE:-minikube}"

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33mWARN: %s\033[0m\n' "$*" >&2; }
die() { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

need() {
  local missing=()
  for tool in "$@"; do
    command -v "${tool}" >/dev/null 2>&1 || missing+=("${tool}")
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    die "missing tools: ${missing[*]} (see infra/k8s/README.md)"
  fi
}

# Read KEY=VALUE from .env without `source`: the file is user-edited and must never be
# executed as shell code. Prints the value of $1 (empty if absent).
env_value() {
  local key="$1" file="${ENV_FILE:-${ROOT}/.env}"
  [[ -f "${file}" ]] || return 0
  grep -E "^${key}=" "${file}" | tail -n 1 | cut -d= -f2- | sed -e 's/^"//' -e 's/"$//'
}

# kustomize renders the platform manifests. The load restrictor must be relaxed because
# generators read contracts/cassandra.cql and the shared Grafana dashboards (see
# infra/k8s/kustomization.yaml).
render_platform() {
  if command -v kustomize >/dev/null 2>&1; then
    kustomize build --load-restrictor LoadRestrictionsNone "${K8S}"
  else
    kubectl kustomize --load-restrictor LoadRestrictionsNone "${K8S}"
  fi
}

# Role-owned deployment folders (A: producers + Kafka Connect, B: SparkApplication).
# They exist only on checkouts that contain those roles' code (an integration branch).
ROLE_K8S_DIRS=("ingest/k8s" "streaming/k8s")

apply_role_dir() {
  local dir="${ROOT}/$1"
  if [[ ! -d "${dir}" ]]; then
    warn "$1 not found in this checkout; skipping (merge role branches into an integration branch)"
    return 0
  fi
  if [[ -f "${dir}/kustomization.yaml" ]]; then
    kubectl apply -k "${dir}"
  else
    kubectl apply -R -f "${dir}"
  fi
}
