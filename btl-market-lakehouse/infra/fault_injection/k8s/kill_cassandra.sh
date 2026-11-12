#!/usr/bin/env bash
# Kịch bản 4: node Cassandra chết (dev chỉ có 1 node).
# Kỳ vọng: market-api trả 503 (DEMO_FALLBACK=false, không hiện số giả), foreachBatch của B
# lỗi rồi query chạy lại từ checkpoint; batch C ghi Cassandra thất bại và Airflow retry.
source "$(dirname "$0")/lib.sh"
POD="$(kubectl -n cassandra get pod -l cassandra.datastax.com/cluster=market -o jsonpath='{.items[0].metadata.name}')"

banner "delete Cassandra pod ${POD}"
kubectl -n cassandra delete pod "${POD}" --wait=false
observe "Market dashboard: ô Nguồn dữ liệu = none, panel báo lỗi 503; market-api p95; Pod khởi động lại"
kubectl -n cassandra wait pod "${POD}" --for=condition=Ready --timeout=1200s
banner "Cassandra ready after $(elapsed)s"
record "thời gian phục hồi=$(elapsed)s; dashboard có tự hồi phục không cần thao tác tay; số batch streaming lỗi"
