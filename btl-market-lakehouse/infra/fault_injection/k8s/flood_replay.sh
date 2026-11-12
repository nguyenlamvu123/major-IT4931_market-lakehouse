#!/usr/bin/env bash
# Kịch bản 6: dồn tải bằng producer replay 100x (slide "Mở rộng": replay vượt sức xử lý).
# Tên Deployment và biến tốc độ do vai A định nghĩa trong ingest/k8s; ghi đè nếu khác:
#   REPLAY_DEPLOYMENT=producer-replay REPLAY_SPEED_ENV=REPLAY_SPEED SPEED=100 REPLICAS=2
source "$(dirname "$0")/lib.sh"
DEPLOYMENT="${REPLAY_DEPLOYMENT:-producer-replay}"
SPEED_ENV="${REPLAY_SPEED_ENV:-REPLAY_SPEED}"
SPEED="${SPEED:-100}"
REPLICAS="${REPLICAS:-2}"

banner "replay ${DEPLOYMENT} at ${SPEED}x with ${REPLICAS} replicas"
kubectl -n market set env "deploy/${DEPLOYMENT}" "${SPEED_ENV}=${SPEED}"
kubectl -n market scale "deploy/${DEPLOYMENT}" --replicas="${REPLICAS}"
observe "Tin nhắn vào Kafka/giây; Spark: dòng vào > dòng xử lý; offsets chưa đọc; CPU/RAM pod"
record "throughput tối đa ổn định (msg/s), offsets chưa đọc lớn nhất, số executor; dừng bằng restore.sh"
