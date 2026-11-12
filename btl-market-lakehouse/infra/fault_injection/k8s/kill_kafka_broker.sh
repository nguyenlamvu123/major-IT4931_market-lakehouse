#!/usr/bin/env bash
# Kịch bản 1: một broker Kafka chết (xoá pod của node pool Strimzi).
# Dev có 1 broker -> producer mất kết nối cho tới khi pod lên lại. Demo 3 broker,
# RF 3, min.insync 2 -> không mất message (slide "Sự cố về hệ thống" của PDF ý tưởng).
source "$(dirname "$0")/lib.sh"
POD="${POD:-$(kubectl -n kafka get pod -l strimzi.io/cluster=market,strimzi.io/broker-role=true -o jsonpath='{.items[0].metadata.name}')}"

banner "delete Kafka broker pod ${POD}"
kubectl -n kafka delete pod "${POD}" --wait=false
observe "Producer: lỗi/kết nối lại tăng; Tin nhắn vào Kafka/giây về 0; Pod khởi động lại"
kubectl -n kafka wait pod "${POD}" --for=condition=Ready --timeout=600s
kubectl -n kafka wait kafka/market --for=condition=Ready --timeout=600s
banner "broker back after $(elapsed)s"
record "thời gian phục hồi=$(elapsed)s; so sánh market_producer_messages_total với số dòng bronze cùng khoảng để đếm message mất; số retry của producer"
