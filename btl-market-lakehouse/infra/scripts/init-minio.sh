#!/usr/bin/env bash
set -euo pipefail

NETWORK="${MINIO_NETWORK:-btl-market-dev_market}"
USER="${MINIO_ROOT_USER:-minioadmin}"
PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"

docker run --rm --network "${NETWORK}" --entrypoint /bin/sh minio/mc -c "
  mc alias set local http://minio:9000 '${USER}' '${PASSWORD}' &&
  mc mb --ignore-existing local/lake &&
  mc mb --ignore-existing local/checkpoints &&
  mc ls local
"
