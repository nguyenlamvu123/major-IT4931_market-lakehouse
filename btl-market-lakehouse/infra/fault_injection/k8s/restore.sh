#!/usr/bin/env bash
# Đưa cụm về trạng thái bình thường sau các kịch bản stop_minio / flood_replay.
source "$(dirname "$0")/lib.sh"
DEPLOYMENT="${REPLAY_DEPLOYMENT:-producer-replay}"

banner "restore MinIO and replay producer"
kubectl -n minio scale deploy/minio --replicas=1
kubectl -n minio rollout status deploy/minio --timeout=300s
if kubectl -n market get "deploy/${DEPLOYMENT}" >/dev/null 2>&1; then
  kubectl -n market scale "deploy/${DEPLOYMENT}" --replicas=0
fi
banner "restored after $(elapsed)s"
record "thời gian để consumer lag và offsets chưa đọc về mức trước sự cố"
