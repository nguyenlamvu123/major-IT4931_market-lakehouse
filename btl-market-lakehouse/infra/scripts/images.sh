#!/usr/bin/env bash
# make images: build the six images of contracts/infra.yaml and load them into minikube.
#
# The cluster runs on a MacBook M1 (PDF, "Máy móc"), so images default to linux/arm64.
# Override for other hosts:  PLATFORM=linux/amd64 make images
# Build only (docker compose full stack, no minikube):  make images-local
#
# Each image is built from the checkout root; an image whose Dockerfile is not in this
# checkout (role code lives on its own branch) is skipped with a warning.
set -euo pipefail
# shellcheck source=infra/scripts/lib.sh
source "$(dirname "$0")/lib.sh"
need docker

PLATFORM="${PLATFORM:-linux/arm64}"
MINIKUBE_LOAD="${MINIKUBE_LOAD:-1}"

# image|Dockerfile (relative to btl-market-lakehouse/)|owner
IMAGES=(
  "btl/ingest:dev|ingest/Dockerfile|A"
  "btl/kafka-connect-s3:dev|ingest/kafka_connect/Dockerfile|A"
  "btl/streaming:dev|streaming/Dockerfile|B"
  "btl/batch:dev|batch/Dockerfile|C"
  "btl/airflow:dev|infra/airflow/Dockerfile|D (needs batch/ from C)"
  "btl/market-api:dev|serving/Dockerfile|D"
)

built=()
for entry in "${IMAGES[@]}"; do
  IFS='|' read -r image dockerfile owner <<<"${entry}"
  if [[ ! -f "${ROOT}/${dockerfile}" ]]; then
    warn "skip ${image}: ${dockerfile} not in this checkout (owner ${owner})"
    continue
  fi
  log "build ${image} (${PLATFORM:-native})"
  if [[ -n "${PLATFORM}" ]]; then
    docker buildx build --platform "${PLATFORM}" -f "${ROOT}/${dockerfile}" -t "${image}" --load "${ROOT}"
  else
    docker build -f "${ROOT}/${dockerfile}" -t "${image}" "${ROOT}"
  fi
  built+=("${image}")
done

if [[ "${MINIKUBE_LOAD}" == "1" && ${#built[@]} -gt 0 ]]; then
  need minikube
  for image in "${built[@]}"; do
    log "minikube image load ${image}"
    minikube -p "${MINIKUBE_PROFILE}" image load "${image}"
  done
fi
echo "built: ${built[*]:-none}"
