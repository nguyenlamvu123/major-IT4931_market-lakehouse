#!/usr/bin/env bash
# Kịch bản 3: mất một executor. Driver xin executor mới; task đang chạy được chạy lại.
source "$(dirname "$0")/lib.sh"
APP="${APP:-speed-layer}"
POD="$(kubectl -n spark get pod -l "sparkoperator.k8s.io/app-name=${APP},spark-role=executor" -o jsonpath='{.items[0].metadata.name}')"

banner "delete executor ${POD} of ${APP}"
kubectl -n spark delete pod "${POD}" --wait=false
observe "Spark streaming: thời gian micro-batch tăng một nhịp; CPU theo pod"
until [[ "$(kubectl -n spark get pod -l "sparkoperator.k8s.io/app-name=${APP},spark-role=executor" \
  --field-selector=status.phase=Running -o name | wc -l)" -ge 1 ]]; do
  sleep 2
done
banner "executor replaced after $(elapsed)s"
record "thời gian phục hồi=$(elapsed)s; micro-batch dài nhất trong lúc sự cố (ms)"
