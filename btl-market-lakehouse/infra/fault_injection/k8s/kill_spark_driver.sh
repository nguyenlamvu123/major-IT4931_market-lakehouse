#!/usr/bin/env bash
# Kịch bản 2: driver Spark Streaming bị kill giữa chừng (PDF việc B.5: chứng minh
# exactly-once). restartPolicy Always -> Spark Operator tạo driver mới, query đọc lại
# offset từ checkpoint s3a://checkpoints/<job>/ và upsert Cassandra theo khóa chính.
#   APP=speed-layer bash infra/fault_injection/k8s/kill_spark_driver.sh
source "$(dirname "$0")/lib.sh"
APP="${APP:-speed-layer}"
SELECTOR="sparkoperator.k8s.io/app-name=${APP},spark-role=driver"
OLD="$(kubectl -n spark get pod -l "${SELECTOR}" -o jsonpath='{.items[0].metadata.name}')"

banner "delete driver ${OLD} of SparkApplication ${APP}"
kubectl -n spark delete pod "${OLD}" --wait=false
observe "Spark streaming: dòng xử lý/giây về 0, offsets chưa đọc tăng; Độ trễ giao dịch -> Cassandra vượt 10 s"
until kubectl -n spark get pod -l "${SELECTOR}" -o jsonpath='{range .items[*]}{.metadata.name} {.status.phase}{"\n"}{end}' \
  | grep -v "^${OLD} " | grep -q " Running$"; do
  sleep 2
done
banner "new driver running after $(elapsed)s"
record "thời gian phục hồi=$(elapsed)s; chạy công cụ đếm trùng của B (streaming/tools) cho ohlcv_1m: số dòng trùng phải = 0"
