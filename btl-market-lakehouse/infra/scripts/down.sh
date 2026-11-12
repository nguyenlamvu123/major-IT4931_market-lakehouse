#!/usr/bin/env bash
# make down: remove everything `make up` created.
#
# Default: delete the minikube profile (PVCs included) so the next `make up` really starts
# from a clean machine, which is what the PDF acceptance test asks for.
# SOFT=1: keep minikube, uninstall Helm releases and delete the project namespaces.
set -euo pipefail
# shellcheck source=infra/scripts/lib.sh
source "$(dirname "$0")/lib.sh"

if [[ "${SOFT:-0}" != "1" ]]; then
  need minikube
  log "minikube delete -p ${MINIKUBE_PROFILE}"
  minikube delete -p "${MINIKUBE_PROFILE}"
  exit 0
fi

need kubectl helm
log "delete role workloads and platform manifests"
for dir in "${ROLE_K8S_DIRS[@]}"; do
  if [[ -d "${ROOT}/${dir}" ]]; then
    kubectl delete -R -f "${ROOT}/${dir}" --ignore-not-found || true
  fi
done
# CRs first, while their operators are still running to finalize them.
render_platform | kubectl delete --ignore-not-found -f - || true

for release_ns in airflow:airflow spark-operator:spark k8ssandra-operator:cassandra \
  strimzi:kafka kube-prometheus-stack:monitoring cert-manager:cert-manager; do
  log "helm uninstall ${release_ns%%:*}"
  helm uninstall "${release_ns%%:*}" -n "${release_ns##*:}" --ignore-not-found || true
done

for namespace in market airflow spark cassandra minio kafka monitoring cert-manager; do
  kubectl delete namespace "${namespace}" --ignore-not-found --wait=false
done
