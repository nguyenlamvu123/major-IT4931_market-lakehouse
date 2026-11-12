#!/usr/bin/env bash
# Kịch bản 5: MinIO (data lake + checkpoint) ngừng hoạt động. Khôi phục bằng restore.sh.
# Kỳ vọng: S3 sink của Kafka Connect retry -> consumer lag tăng nhưng Kafka giữ dữ liệu
# (retention 7 ngày) nên không mất; streaming không ghi được checkpoint -> query dừng.
source "$(dirname "$0")/lib.sh"

banner "scale minio to 0"
kubectl -n minio scale deploy/minio --replicas=0
observe "Kafka consumer lag (Kafka Connect -> MinIO bronze) tăng dần; Spark streaming dừng"
record "lag lớn nhất; sau restore.sh: thời gian để lag về 0, số file bronze bị thiếu/trùng"
