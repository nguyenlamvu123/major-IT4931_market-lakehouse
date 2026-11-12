# Kiến trúc (hợp đồng v2)

Hệ thống theo kiến trúc Lambda (bài giảng L09), bám PDF *Kiến trúc – Phân công*
(09/11/2026). Một luồng dữ liệu vào Kafka, sau đó tách làm hai:

- **Tầng tốc độ:** độ trễ vài giây.
- **Tầng khối:** chạy mỗi đêm.

Hai tầng gặp nhau ở Cassandra, rồi được hiển thị trên Grafana. Mọi thành phần chạy trên
Kubernetes (minikube).

Nguồn sự thật cho tên topic, schema, đường dẫn, bảng và hạ tầng nằm ở `contracts/`
(`topics.yaml`, `schemas/`, `paths.yaml`, `cassandra.cql`, `infra.yaml`).

## Sơ đồ

```text
 A  Thu thập & Kafka           B  Speed layer              C  Batch layer             D  Hạ tầng & Dashboard

 Binance (crypto 24/7) ─┐
 Alpaca/Finnhub (cổ     │   ┌────────────── Kafka "market" (Strimzi, ns kafka) ──────────────┐
  phiếu, GLD, TLT/IEF/  ├──►│ crypto.trades  stocks.trades  stocks.bars  gold.quotes  fx.dxy │
  SHY)                  │   │ rates.yields   alerts (B ghi)  dead-letter                     │
 Yahoo (GC=F, DXY,      │   └───────┬───────────────────────────────────────┬─────────────────┘
  backfill 1Day)        │           │                                       │ KafkaConnect S3 sink (A)
 Treasury CSV / FRED ───┘           ▼                                       ▼
 Replay 1x..100x              Spark Structured Streaming (B)     MinIO s3a://lake (ns minio)
                              ns spark, SparkApplication         bronze/<topic>/dt=YYYY-MM-DD/
                              nến 1m/5m, VWAP, chỉ báo,                 │  Airflow 01:00 UTC (C)
                              cảnh báo, watermark, checkpoint           ▼  KubernetesExecutor
                              s3a://checkpoints/<job>/          Spark batch (C, ns spark)
                                    │                           silver/ → kiểm tra chất lượng → gold/
                                    │                           ngày, tương quan + lợi suất,
                                    │                           đường cong lợi suất, MLlib KMeans
                                    ▼                                       │
                     ┌──────── Cassandra keyspace market (K8ssandra, ns cassandra) ────────┐
                     │ realtime ← B: ohlcv_1m, ohlcv_5m, indicators_latest, alerts_by_day  │
                     │ batch    ← C: daily_summary, correlation_matrix, yield_curve,       │
                     │              asset_clusters                                         │
                     └──────────────────────────────┬──────────────────────────────────────┘
                                                    ▼
                          market-api (D, ns market) ──Infinity──► Grafana (ns monitoring)
                          Prometheus: kafka-exporter, producer :8001, driver :9108, cAdvisor ─┘
```

Đọc từ trái sang phải. Chữ cái A–D là thành viên phụ trách. Cassandra là điểm gặp nhau
của hai luồng (PDF mục 2).

## Thành phần

| Tầng | Thành phần | Vai | Namespace | Ghi chú |
|---|---|---|---|---|
| Thu thập | Producer Python (Binance, Alpaca, Finnhub, Yahoo, Treasury/FRED, replay, backfill) | A | `market` | Key = `symbol` (`tenor` cho lợi suất); `trade_id` tất định; metric ở cổng 8001 |
| Hàng đợi | Kafka `market` (Strimzi, KRaft, 3.9.0) | D dựng, A dùng | `kafka` | `market-kafka-bootstrap.kafka.svc:9092`; topic sinh từ `contracts/topics.yaml` |
| Đổ về hồ dữ liệu | KafkaConnect `market-connect` + S3 sink | A | `kafka` | Ghi `bronze/<topic>/dt=…` theo `event_time` |
| Tầng tốc độ | Spark Structured Streaming | B | `spark` | Nến 1m/5m, VWAP, chỉ báo, cảnh báo, checkpoint trên MinIO, metric ở cổng 9108 |
| Hồ dữ liệu | MinIO (S3, thay HDFS) | D | `minio` | Bucket `lake`, `checkpoints` |
| Tầng khối | Spark batch + MLlib | C | `spark` | bronze → silver → gold, tương quan, đường cong lợi suất, KMeans |
| Điều phối | Airflow (KubernetesExecutor) | C (DAG), D (cài đặt) | `airflow` | `0 1 * * *` UTC, `dt = {{ ds }}` |
| Phục vụ | Cassandra (K8ssandra `market`, dc1) | D dựng, B/C ghi | `cassandra` | `market-dc1-service.cassandra.svc:9042` |
| Hiển thị | market-api → Grafana (Infinity) | D | `market`, `monitoring` | `DEMO_FALLBACK=false` trên cụm |
| Giám sát | kube-prometheus-stack | D | `monitoring` | Kafka lag, tốc độ Spark, CPU/RAM |

## Ngữ nghĩa dữ liệu

- **Chống trùng ở đầu vào:** producer idempotent (`acks=all`). Gửi lại cùng một sự kiện
  cho ra cùng `trade_id`/`bar_id`. Silver bỏ trùng theo id.
- **Luồng nhanh:**
  - Xử lý theo event time, có watermark.
  - Offset Kafka lưu trong checkpoint `s3a://checkpoints/<job>/` (Spark không commit offset
    vào Kafka).
  - Ghi Cassandra là upsert theo khóa chính, nên chạy lại một micro-batch không tạo dòng
    trùng. Kịch bản kiểm chứng ở `docs/lessons/D-fault-injection.md` (#2).
- **Luồng chậm:** mỗi đêm ghi đè đúng phân vùng `dt` đang xử lý, nên chạy lại DAG cho cùng
  một ngày cho ra cùng kết quả.
- **Hai luồng lệch nhau:** bảng realtime có TTL 7 ngày. Số liệu theo ngày lấy từ batch
  (chính xác hơn, tính lại trên toàn bộ dữ liệu) là con số cuối cùng.
- **Lịch giao dịch khác nhau:**
  - Lịch chuẩn là các ngày SPY giao dịch.
  - Crypto chạy 24/7, nên bỏ các ngày cuối tuần khi join.
  - Lợi suất được forward-fill tối đa 3 ngày vào các ngày thị trường trái phiếu nghỉ riêng.

## Môi trường chạy

| Chế độ | Lệnh | Cần | Dùng khi |
|---|---|---|---|
| Demo local | `make demo`, `make api` | Python | Phát triển logic, không cần Docker |
| Compose dev | `make dev-up && make topics` | Docker, khoảng 6 GB RAM | A/B/C phát triển trên laptop (PDF việc D.4) |
| Compose full | `docker compose -f infra/docker-compose.full.yml up -d` | Khoảng 12–16 GB RAM | Máy không có minikube |
| Kubernetes | `make up` | minikube 6 CPU / 16 GB (MacBook M1 Max) | Cụm chính, demo, đo số liệu |

## Giới hạn đã biết

- Cấu hình dev có 1 broker và 1 node Cassandra, chưa bật auth hay TLS. Cách nâng lên 3
  broker / 3 node có trong `infra/k8s/README.md`.
- Chưa có schema registry: hợp đồng được kiểm bằng JSON Schema và test (`tests/test_contracts.py`).
- Grafana đọc Cassandra qua market-api, vì không có datasource Cassandra chính thức.
