# BTL IT4931 - Market Lakehouse

Nền tảng giám sát thị trường tài chính thời gian thực, chia theo 4 vai A/B/C/D.

## 1. Hệ thống làm gì

- Thu thập giá cổ phiếu Mỹ, vàng, crypto và lợi suất trái phiếu Mỹ.
- Luồng nhanh: Spark Structured Streaming tạo nến 1 phút, VWAP, chỉ báo, cảnh báo; ghi Cassandra.
- Luồng chậm: Spark batch làm sạch dữ liệu trong MinIO, tính số liệu ngày, tương quan, yield curve, MLlib.
- Dashboard Grafana đọc dữ liệu realtime/batch; Prometheus giám sát hạ tầng.

## 2. Phân vai và branch

| Vai | Phạm vi | Branch lớn | Branch chức năng |
|---|---|---|---|
| A | Thu thập dữ liệu & Kafka | `feature/A-ingest` | `feature/A-contracts`, `feature/A-producers`, `feature/A-kafka-connect` |
| B | Speed layer | `feature/B-streaming` | `feature/B-ohlcv`, `feature/B-indicators`, `feature/B-cassandra`, `feature/B-job` |
| C | Batch layer & Data Lake | `feature/C-batch` | `feature/C-medallion`, `feature/C-mllib`, `feature/C-airflow` |
| D | Hạ tầng & Dashboard | `feature/D-infra` | `feature/D-compose`, `feature/D-k8s`, `feature/D-dashboards`, `feature/D-ci` |

Chi tiết lịch sử branch: `docs/branching.md`.

## 3. Cấu trúc repo

```text
btl-market-lakehouse/
├── contracts/          # topic, schema, sample, Cassandra CQL
├── ingest/             # A: producers, Kafka Connect, Dockerfile, K8s
├── streaming/          # B: Spark Structured Streaming
├── batch/              # C: Spark batch, Airflow, MLlib
├── infra/              # D: Docker Compose, K8s, Prometheus, Grafana
├── serving/            # D: API cầu nối Cassandra -> Grafana
├── scripts/            # demo, seed, verify
├── tests/              # test tích hợp nhẹ
├── docs/               # kiến trúc, runbook, metrics, báo cáo
├── Makefile
└── requirements.txt
```

## 4. Chạy nhanh trên máy không có Spark/Kafka

```bash
cd btl-market-lakehouse
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
make samples          # sinh dữ liệu mẫu trong contracts/samples
make demo             # chạy pipeline mô phỏng, ghi data/demo
make test             # chạy unit test
```

`make demo` không cần Docker, Kafka, Spark hay Cassandra. Nó dùng pandas để mô phỏng
đúng logic OHLCV/indicator/alert và ghi JSON/CSV để kiểm tra.

## 5. Chạy hạ tầng dev bằng Docker

```bash
cp .env.example .env
make dev-up           # Kafka + MinIO + Cassandra
make topics           # tạo topic
make cassandra-init   # tạo keyspace/table
```

Lưu ý: máy hiện tại chỉ còn ~3.6 GB đĩa và ~7.6 GB RAM, không đủ chạy full stack
Spark/Airflow/Prometheus/Grafana. Dùng `make demo` để kiểm tra logic; chạy full stack
trên máy có ít nhất 16 GB RAM.

## 6. Chạy Spark thật

```bash
# Streaming (B)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  streaming/job.py --bootstrap localhost:9092 --checkpoint s3a://checkpoints/ohlcv

# Batch (C)
spark-submit batch/jobs/bronze_to_silver.py
spark-submit batch/jobs/silver_to_gold.py
```

## 7. Hợp đồng dữ liệu

Mọi thay đổi topic/schema/đường dẫn/bảng Cassandra phải sửa trong `contracts/` và
được ít nhất một người khác duyệt. Xem `contracts/README.md`.

## 8. Giới hạn của bản MVP

- Code và cấu hình đã đủ để chạy demo/local; chưa test tích hợp thật với cụm K8s.
- Các API cần key (Alpaca, Finnhub, FRED) dùng biến môi trường; nếu thiếu key, dùng
  producer mock/replay.
- Grafana dashboard dùng `market-api` làm cầu nối JSON; cần cài Infinity datasource
  hoặc dùng Prometheus cho dashboard giám sát.
