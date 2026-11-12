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

Chi tiết lịch sử branch: `docs/branching.md`. Các nhánh con theo PDF (hợp đồng v2) và nội
dung từng nhánh: `docs/thay-doi-theo-pdf.md`.

## 3. Cấu trúc repo

```text
btl-market-lakehouse/
├── contracts/          # hợp đồng v2 trên main: topics, schemas, samples, cassandra.cql, paths, infra
├── ingest/             # A: producers, Kafka Connect, Dockerfile, K8s
├── streaming/          # B: Spark Structured Streaming
├── batch/              # C: Spark batch, Airflow, MLlib
├── infra/              # D: Docker Compose, K8s, Prometheus, Grafana
├── serving/            # D: API cầu nối Cassandra -> Grafana
├── scripts/            # demo, seed, verify
├── tests/              # test common/ + đồng bộ hợp đồng (test_contracts.py)
├── docs/               # kiến trúc, runbook, metrics, báo cáo, thay-doi-theo-pdf.md
├── Makefile
└── requirements.txt
```

`.github/` (CI, CODEOWNERS, mẫu PR) nằm ở **gốc git** (`../.github`), vì GitHub chỉ đọc
thư mục đó; CI chạy với `working-directory: btl-market-lakehouse`.

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

Cụm K8s đầy đủ (`make up` trên nhánh D) cần khoảng 16 GB RAM cho minikube (MacBook M1
Max theo PDF). Máy yếu dùng Docker Compose dev (Kafka + MinIO + Cassandra) và Spark local.

## 6. Chạy Spark thật

Lệnh chạy nằm trong README của từng vai (cần Java 17):

- Streaming (B): `streaming/README.md` trên `feature/B-streaming` — đọc file mẫu
  (`--source files`) hoặc Kafka; nến 1m/5m chế độ update; SparkApplication `speed-layer`.
- Batch (C): `batch/README.md` trên `feature/C-batch` — `bronze_to_silver` →
  `quality_checks` → `silver_to_gold` → `cassandra_sink` → `train_kmeans`, mỗi job nhận `--dt`.

## 7. Hợp đồng dữ liệu (v2, theo PDF Kiến trúc – Phân công)

- Topic: `crypto.trades`, `stocks.trades`, `stocks.bars`, `gold.quotes`, `fx.dxy`,
  `rates.yields`, `alerts` (+ `dead-letter`); Kafka `market-kafka-bootstrap.kafka.svc:9092`.
- MinIO: `s3a://lake/{bronze,silver,gold}/<bảng>/dt=YYYY-MM-DD/`, `s3a://checkpoints/<job>/`.
- Cassandra keyspace `market`: realtime `ohlcv_1m`, `ohlcv_5m`, `indicators_latest`,
  `alerts_by_day`; batch `daily_summary`, `correlation_matrix`, `yield_curve`, `asset_clusters`.

Mọi thay đổi topic/schema/đường dẫn/bảng Cassandra phải sửa trong `contracts/` (trên
`main`) và được vai nhận dữ liệu duyệt. `python -m pytest tests/test_contracts.py` bắt lệch
giữa `contracts/` và `common/contracts.py`. Xem `contracts/README.md`.

## 8. Giới hạn hiện tại

- Logic đã có test (gồm test Spark local); chưa chạy trên cụm K8s/Docker thật. Danh sách
  việc cần cụm thật và rủi ro: `docs/thay-doi-theo-pdf.md` mục 7–8.
- API cần key (Alpaca, Finnhub, FRED) đọc từ `.env`/Secret; thiếu key thì dùng nguồn không
  cần key (Binance, Yahoo, CSV Treasury) hoặc replay.
- Grafana đọc Cassandra qua `market-api` (plugin Infinity); dashboard giám sát đọc Prometheus.
