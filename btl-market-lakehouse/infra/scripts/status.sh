#!/usr/bin/env bash
# make status: one screen with the state of every layer.
set -euo pipefail
# shellcheck source=infra/scripts/lib.sh
source "$(dirname "$0")/lib.sh"
need kubectl

for namespace in kafka minio cassandra spark airflow monitoring market; do
  log "ns ${namespace}"
  kubectl -n "${namespace}" get pods -o wide 2>/dev/null || warn "namespace ${namespace} missing"
done
log "custom resources"
kubectl -n kafka get kafka,kafkanodepools,kafkatopics 2>/dev/null || true
kubectl -n cassandra get k8ssandraclusters 2>/dev/null || true
kubectl -n spark get sparkapplications 2>/dev/null || true
kubectl get jobs -A 2>/dev/null | grep -E 'minio-buckets|cassandra-schema' || true
