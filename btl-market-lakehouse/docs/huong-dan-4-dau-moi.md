# Hướng dẫn điều phối 4 đầu mối A/B/C/D

> Dành cho trưởng nhóm và 4 đầu mối vai A, B, C, D.
> Mục tiêu: mỗi người biết mình sở hữu gì, làm trên branch nào, kiểm tra bằng lệnh gì,
> bàn giao cho ai và không phá vỡ quy ước chung của repo.
>
> Cập nhật: 2026-11-12.

## 0. Mục tiêu và nguyên tắc bất di bất dịch

1. `main` chỉ chứa phần nền dùng chung: `common/`, `scripts/`, `tests/`, `docs/`,
   `README.md`, `Makefile`, cấu hình Python.
2. Có **4 branch lớn**, mỗi vai sở hữu một branch:
   `feature/A-ingest`, `feature/B-streaming`, `feature/C-batch`, `feature/D-infra`.
3. Branch con chỉ merge vào branch lớn tương ứng. **Không merge branch vai vào `main`**.
4. Không merge branch vai này vào branch vai khác. Nếu cần tích hợp, tạo riêng branch
   `integration` và merge ở đó sau khi review.
5. `contracts/` là nguồn sự thật cho topic, schema, sample và Cassandra CQL. Thay đổi
   contract phải được ít nhất một vai khác review.
6. Không commit `.env`, API key, dữ liệu runtime trong `data/`, `__pycache__/`,
   checkpoint, log hoặc file dung lượng lớn.
7. Máy hiện tại **không đủ tài nguyên chạy full stack**. Ưu tiên local demo,
   `pytest`, `docker compose config`; chỉ chạy full stack trên máy đủ RAM/đĩa.

## 1. Bản đồ branch và quyền sở hữu

| Vai | Branch lớn | Branch con | Thư mục sở hữu | Bàn giao chính |
|---|---|---|---|---|
| A | `feature/A-ingest` | `A-contracts`, `A-producers`, `A-kafka-connect` | `contracts/`, `ingest/` | Topic/schema/sample cho B, bronze cho C |
| B | `feature/B-streaming` | `B-ohlcv`, `B-indicators`, `B-cassandra`, `B-job` | `streaming/` | Bảng Cassandra/alert cho D |
| C | `feature/C-batch` | `C-medallion`, `C-mllib`, `C-airflow` | `batch/` | Gold data/ML cho D, báo cáo cho nhóm |
| D | `feature/D-infra` | `D-compose`, `D-k8s`, `D-dashboards`, `D-ci` | `infra/`, `serving/`, `.github/`, `docs/` | Runtime, API, dashboard, CI, runbook |

### 1.1. Luồng dữ liệu tổng quát

```text
Nguồn dữ liệu
  -> A: producers -> Kafka -> A: Kafka Connect S3 sink -> MinIO bronze
  -> B: Spark Structured Streaming -> Cassandra (bars, alerts, latest_price)
  -> C: Spark batch -> MinIO silver/gold (daily, correlation, yield, ML)
  -> D: FastAPI -> Grafana; Prometheus giám sát hạ tầng
```

### 1.2. Trạng thái hiện tại

- Cả 4 branch lớn đã có code và đã merge các branch con.
- `main` đang giữ phần nền, không chứa code của A/B/C/D.
- D đã có Compose, K8s, API, Prometheus/Grafana, CI, tài liệu và fault injection.
- Full stack chưa chạy được trên máy hiện tại vì đĩa còn khoảng 3.6 GB; các file
  Compose/K8s đã được validate.

## 2. Chuẩn bị chung cho cả 4 đầu mối

### 2.1. Vào đúng thư mục

```bash
cd "/media/zaibachkhoa/education/GenAI 2025/Lưu trữ và xử lí dữ liệu lớn/btl-market-lakehouse"
git status
git branch --show-current
```

### 2.2. Cài môi trường tối thiểu

Không nên cài toàn bộ `requirements.txt` nếu máy thiếu đĩa, vì có `pyspark`,
`confluent-kafka`, `yfinance`... Trên máy hiện tại dùng bộ tối thiểu:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pandas pyarrow numpy requests python-dotenv PyYAML pytest \
  fastapi "uvicorn[standard]" prometheus-client
```

Nếu cần chạy producer/Kafka thật, cài thêm theo hướng dẫn của vai A. Nếu cần Spark,
chỉ chạy trên máy đã có Java và đủ tài nguyên.

### 2.3. Kiểm tra nền dùng chung

Chạy trên branch bất kỳ:

```bash
make demo
make verify
make test
```

Kết quả demo mong đợi:

- `data/demo/trades.jsonl`
- `data/demo/bars.jsonl`
- `data/demo/alerts.json`
- `data/demo/daily_summary.csv`
- `data/demo/correlation_matrix.csv`
- `data/demo/yield_curve.csv`
- `data/demo/ml_features.csv`
- `data/demo/summary.json`

`make demo` không cần Kafka, Spark, Cassandra hay Docker. Đây là bài kiểm tra nhanh
nhất để biết branch có làm hỏng logic dùng chung hay không.

## 3. Quy trình branch, commit và merge

### 3.1. Tạo branch con

Luôn tạo branch con từ branch lớn hiện tại:

```bash
git checkout feature/A-ingest
git pull --ff-only 2>/dev/null || true
git checkout -b feature/A-ten-task
```

Ví dụ tên branch: `feature/A-producers`, `feature/B-cassandra`,
`feature/C-mllib`, `feature/D-dashboards`.

### 3.2. Commit

```bash
git status
git diff
git add <các-file-cần-thiết>
git -c user.name='BTL Team' -c user.email='nguyenlamvu123@gmail.com' \
  commit -m "feat(B-indicators): thêm RSI/MACD và alert z-score"
```

Quy ước message:

- `feat(<vai>-<phần>): ...` cho tính năng mới.
- `fix(<vai>-<phần>): ...` cho sửa lỗi.
- `docs(<vai>-<phần>): ...` cho tài liệu.
- Không commit file rác, secret, dữ liệu runtime.

### 3.3. Merge branch con vào branch lớn

```bash
git checkout feature/A-ingest
git merge --no-ff feature/A-ten-task -m "merge(A): <mô tả>"
```

Sau merge:

```bash
git log --oneline --graph --decorate -10
git status
```

### 3.4. Kiểm tra trước khi bàn giao

```bash
python -m pytest
python -m compileall -q common scripts tests <thư-mục-vai>
```

Nếu vai có Docker/K8s:

```bash
docker compose -f infra/docker-compose.dev.yml config -q
docker compose -f infra/docker-compose.full.yml config -q
```

### 3.5. Quy tắc review

- A thay đổi `contracts/` phải được B hoặc C review.
- B thay đổi schema đọc/ghi Cassandra phải khớp `contracts/cassandra/schema.cql`.
- C thay đổi đường dẫn gold/silver phải thông báo D và cập nhật README.
- D thay đổi port, biến môi trường, endpoint API phải thông báo A/B/C.
- Không merge thẳng vào `main`; không force-push branch đã chia sẻ.

## 4. Đầu mối A - Thu thập dữ liệu và Kafka

### 4.1. Phạm vi

- Sở hữu `contracts/` và `ingest/`.
- Định nghĩa topic, JSON schema, sample, Cassandra CQL.
- Viết producer cho crypto, equity, vàng, FX, lợi suất; hỗ trợ `mock`/`replay`.
- Cấu hình Kafka Connect S3 sink ghi bronze.

### 4.2. File quan trọng

| File | Vai trò |
|---|---|
| `contracts/topics.yaml` | Tên topic, partition, retention |
| `contracts/schemas/trade.schema.json` | Contract trade |
| `contracts/schemas/rate_tick.schema.json` | Contract rate |
| `contracts/schemas/alert.schema.json` | Contract alert |
| `contracts/cassandra/schema.cql` | Bảng Cassandra dùng chung |
| `contracts/samples/*.jsonl` | Dữ liệu mẫu cho replay/test |
| `ingest/run_producer.py` | Entrypoint producer |
| `ingest/producers/*.py` | Binance, Alpaca, Finnhub, Yahoo, FRED, mock, replay |
| `ingest/kafka_connect/s3-sink.json` | Connector S3 |
| `ingest/README.md` | Hướng dẫn chi tiết |

### 4.3. Lệnh kiểm tra

Dry-run, không cần Kafka:

```bash
git checkout feature/A-ingest
python ingest/run_producer.py --source mock --rows 20 --dry-run
python ingest/run_producer.py --source replay \
  --file contracts/samples/equity.trades.jsonl --dry-run
python -m pytest ingest/tests -q
```

Chạy với Kafka:

```bash
export KAFKA_BOOTSTRAP=localhost:9092
python ingest/run_producer.py --source binance --symbols BTCUSDT,ETHUSDT
python ingest/run_producer.py --source fred --interval 3600
```

Đăng ký S3 sink (sau khi có Connect image có plugin S3):

```bash
bash ingest/kafka_connect/register_connector.sh
```

### 4.4. Điều kiện hoàn thành

- [ ] Topic/schema/CQL được review bởi B hoặc C.
- [ ] `mock` và `replay` dry-run chạy được.
- [ ] `pytest ingest/tests` pass.
- [ ] Producer dùng key `symbol`/`tenor`, đảm bảo thứ tự theo symbol.
- [ ] `trade_id` deterministic, hỗ trợ ghi idempotent.
- [ ] Không có API key trong code; chỉ dùng biến môi trường.
- [ ] Sample JSONL khớp schema.
- [ ] README ghi rõ source matrix và cách chạy.

### 4.5. Bàn giao

- Cho B: tên topic, key, schema, sample, tần suất, watermark mong đợi.
- Cho C: format bronze, đường dẫn/partition, `trade_id`, sample rates.
- Cho D: danh sách service/port/env cần dùng cho Compose/K8s.

### 4.6. Rủi ro

- Schema thay đổi đột ngột làm B/C đọc sai.
- Producer thiếu key làm mất thứ tự theo symbol.
- Rate limit/API key của nguồn ngoài; luôn có `mock`/`replay` dự phòng.

## 5. Đầu mối B - Speed layer / Streaming

### 5.1. Phạm vi

- Sở hữu `streaming/`.
- Đọc Kafka, tạo nến 1 phút, VWAP, chỉ báo, alert.
- Ghi Cassandra và có thể ghi alert topic.
- Quản lý checkpoint, watermark, chế độ Parquet khi không có Cassandra.

### 5.2. File quan trọng

| File | Vai trò |
|---|---|
| `streaming/job.py` | Entrypoint Spark Structured Streaming |
| `streaming/schemas.py` | Schema đọc Kafka |
| `streaming/ohlcv.py` | Logic nến 1 phút |
| `streaming/batch_logic.py` | Logic indicator/alert cho từng micro-batch |
| `streaming/indicators.py` | Wrapper chỉ báo |
| `streaming/alerts.py` | Luật alert |
| `streaming/cassandra_sink.py` | Ghi Cassandra idempotent |
| `streaming/tests/` | Test logic không cần Spark |
| `streaming/README.md` | Hướng dẫn chi tiết |

### 5.3. Lệnh kiểm tra

```bash
git checkout feature/B-streaming
python -m pytest streaming/tests -q
```

Chạy Spark thật (cần Java/Spark/Kafka):

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  streaming/job.py \
  --bootstrap localhost:9092 \
  --checkpoint s3a://checkpoints/speed \
  --cassandra-host cassandra
```

Chạy không cần Cassandra, ghi Parquet:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  streaming/job.py \
  --checkpoint /tmp/checkpoints/speed \
  --output-mode parquet \
  --parquet-path /tmp/speed/bars
```

### 5.4. Điều kiện hoàn thành

- [ ] Schema đọc khớp `contracts/schemas/trade.schema.json`.
- [ ] OHLCV 1 phút có open/high/low/close/volume/vwap/trade_count.
- [ ] Indicator/alert chạy đúng trên batch logic test.
- [ ] Cassandra ghi upsert theo khóa chính, replay không tạo bản ghi trùng.
- [ ] Có watermark 2 phút và `failOnDataLoss=false`.
- [ ] Checkpoint được cấu hình rõ.
- [ ] README ghi rõ chế độ Cassandra/Parquet và giới hạn.
- [ ] `pytest streaming/tests` pass.

### 5.5. Bàn giao

- Cho D: keyspace, bảng, cột, kiểu dữ liệu, endpoint cần đọc.
- Cho nhóm: cách chạy, checkpoint, package Spark, hành vi khi mất dữ liệu.
- Cho A: phản hồi nếu schema/key không đúng.

### 5.6. Rủi ro

- Không có Java/Spark trên máy hiện tại: chỉ test logic, không chạy job thật.
- Indicator tính theo micro-batch chưa giữ state dài hạn; ghi rõ trong báo cáo.
- Event trễ hơn watermark bị drop; cần nêu trong phần giới hạn.

## 6. Đầu mối C - Batch layer / Data Lake

### 6.1. Phạm vi

- Sở hữu `batch/`.
- Bronze -> silver -> gold; dedupe, daily bars, correlation, yield curve, ML features.
- MLlib KMeans cho regime tài sản.
- Airflow DAG điều phối batch.

### 6.2. File quan trọng

| File | Vai trò |
|---|---|
| `batch/jobs/bronze_to_silver.py` | Làm sạch, dedupe, ghi silver |
| `batch/jobs/silver_to_gold.py` | Daily/correlation/yield/features |
| `batch/jobs/train_kmeans.py` | MLlib KMeans |
| `batch/run_local_batch.py` | Chạy toàn bộ batch bằng pandas |
| `batch/transforms.py` | Logic transform dùng chung |
| `batch/ml_features.py` | Feature engineering |
| `batch/dags/market_lakehouse_dag.py` | DAG Airflow |
| `batch/tests/` | Test transform/DAG/ML features |
| `batch/README.md` | Hướng dẫn chi tiết |

### 6.3. Lệnh kiểm tra

Local, không cần Spark/MinIO:

```bash
git checkout feature/C-batch
python batch/run_local_batch.py
python -m pytest batch/tests -q
```

Output local:

- `data/batch/silver/trades.jsonl`
- `data/batch/gold/daily_bars.csv`
- `data/batch/gold/correlation_matrix.csv`
- `data/batch/gold/ml_features.csv`
- `data/batch/gold/yield_curve.csv`

Chạy Spark thật:

```bash
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  batch/jobs/bronze_to_silver.py \
  --bronze s3a://lake/bronze \
  --silver s3a://lake/silver/trades \
  --minio-endpoint http://minio:9000 \
  --access-key minioadmin --secret-key minioadmin

spark-submit batch/jobs/silver_to_gold.py \
  --silver s3a://lake/silver/trades \
  --gold s3a://lake/gold \
  --rates s3a://lake/bronze/rates.ticks \
  --minio-endpoint http://minio:9000 \
  --access-key minioadmin --secret-key minioadmin
```

Airflow:

```bash
export MARKET_PROJECT_DIR=/opt/app
export MINIO_ENDPOINT=http://minio:9000
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin

cp batch/dags/market_lakehouse_dag.py "$AIRFLOW_HOME/dags/"
airflow dags list | grep market_lakehouse
airflow dags trigger market_lakehouse_batch
```

### 6.4. Điều kiện hoàn thành

- [ ] Silver dedupe theo `trade_id`; chạy lại không nhân đôi dữ liệu.
- [ ] Gold có daily bars, correlation, yield curve, ML features.
- [ ] Local batch chạy được không cần Spark.
- [ ] `pytest batch/tests` pass, gồm test DAG import.
- [ ] DAG đúng thứ tự: bronze -> silver -> gold -> KMeans -> quality.
- [ ] README ghi rõ output và giới hạn.
- [ ] Không hard-code credential; dùng env.

### 6.5. Bàn giao

- Cho D: đường dẫn gold, tên cột, tần suất cập nhật, metric/report cần hiển thị.
- Cho nhóm: số liệu demo, cách chạy local, cách deploy DAG.
- Cho A: xác nhận format bronze và sample rates.

### 6.6. Rủi ro

- Bronze format thay đổi làm silver fail.
- ML features ít dòng; KMeans chỉ mang tính demo.
- Airflow/Spark chưa chạy thật trên máy hiện tại; cần nêu rõ khi báo cáo.

## 7. Đầu mối D - Hạ tầng, API, Dashboard, CI

### 7.1. Phạm vi

- Sở hữu `infra/`, `serving/`, `.github/workflows/`, tài liệu vận hành.
- Compose dev/full, script init topic/Cassandra/MinIO.
- K8s manifests cho Kafka, MinIO, Cassandra, Spark, Airflow, API, monitoring.
- FastAPI đọc Cassandra, fallback demo khi Cassandra chưa sẵn sàng.
- Prometheus/Grafana provisioning.
- CI, smoke test, fault injection, runbook.

### 7.2. File quan trọng

| File | Vai trò |
|---|---|
| `infra/docker-compose.dev.yml` | Kafka + MinIO + Cassandra |
| `infra/docker-compose.full.yml` | Full stack |
| `infra/scripts/init-*.sh` | Tạo topic, keyspace, bucket |
| `infra/k8s/*.yaml` | Manifests K8s + kustomization |
| `serving/market_api.py` | FastAPI |
| `serving/cassandra_repo.py` | Đọc Cassandra, retry/fallback |
| `serving/demo_repo.py` | Dữ liệu demo |
| `infra/monitoring/` | Prometheus + Grafana dashboards |
| `.github/workflows/ci.yml` | CI |
| `scripts/smoke_test.sh` | Smoke test API/Prometheus/Grafana |
| `docs/runbook.md` | Vận hành và xử lý sự cố |

### 7.3. Lệnh kiểm tra

Validate cấu hình, không cần tải image:

```bash
git checkout feature/D-infra
docker compose -f infra/docker-compose.dev.yml config -q
docker compose -f infra/docker-compose.full.yml config -q
python -m pytest -q
```

Parse K8s và dashboard:

```bash
python - <<'PY'
import glob, json, yaml
for p in glob.glob("infra/monitoring/grafana/dashboards/*.json"):
    json.load(open(p))
for p in glob.glob("infra/k8s/*.yaml"):
    list(yaml.safe_load_all(open(p, encoding="utf-8")))
print("OK")
PY
```

Chạy API local:

```bash
pip install -r serving/requirements.txt
python -m uvicorn serving.market_api:app --port 8000
curl localhost:8000/health
curl "localhost:8000/api/v1/bars?symbol=BTCUSDT&limit=5"
curl "localhost:8000/api/v1/alerts?symbol=BTCUSDT&limit=5"
```

Chạy dev stack (chỉ khi đủ tài nguyên):

```bash
cp infra/.env.example .env
docker compose -f infra/docker-compose.dev.yml up -d
bash infra/scripts/init-topics.sh
bash infra/scripts/init-cassandra.sh
bash infra/scripts/init-minio.sh
```

Build API và full stack:

```bash
docker build -f serving/Dockerfile -t btl/market-api .
docker compose -f infra/docker-compose.full.yml up -d
```

K8s:

```bash
kubectl apply -k infra/k8s
kubectl -n market get pods,svc
kubectl -n market port-forward svc/grafana 3000:3000
```

Smoke test khi stack đã chạy:

```bash
bash scripts/smoke_test.sh
```

### 7.4. Điều kiện hoàn thành

- [ ] Cả hai file Compose `config -q` pass.
- [ ] K8s YAML parse pass; `kustomization.yaml` trỏ đúng file.
- [ ] Grafana dashboards JSON hợp lệ; datasource/provisioning đúng.
- [ ] API `/health`, `/api/v1/bars`, `/api/v1/alerts`, `/metrics` chạy được.
- [ ] API fallback demo khi không có Cassandra và tự retry mỗi 5 giây.
- [ ] CI chạy test/compile/demo/compose config.
- [ ] Smoke test và fault injection có hướng dẫn.
- [ ] Runbook, architecture, metrics, report outline đầy đủ.
- [ ] Không đưa credential thật vào manifest/compose.

### 7.5. Bàn giao

- Cho nhóm: port, service name, env, lệnh chạy/kiểm tra.
- Cho B: API đọc bảng/cột nào; nếu B đổi schema, báo D.
- Cho C: dashboard/report cần gold data nào.
- Cho A: service Kafka/MinIO dùng trong Compose/K8s.

### 7.6. Rủi ro và giới hạn

- Full stack cần khoảng 12-16 GB RAM và 12+ GB đĩa; máy hiện tại không đủ.
- K8s manifests dùng single-node Kafka/Cassandra, chưa production-ready.
- API chỉ đọc; không có ghi/streaming qua API.
- Grafana bảng dùng Infinity plugin; nếu chạy ngoài Compose/K8s phải cài plugin.
- API có thể trả danh sách rỗng trong một request đúng lúc Cassandra vừa mất kết nối;
  request sau sẽ fallback demo hoặc kết nối lại.

## 8. Luồng bàn giao và tích hợp

### 8.1. Bàn giao theo cặp

| Từ | Đến | Artifact | Điều kiện |
|---|---|---|---|
| A | B | `contracts/topics.yaml`, schema, sample, key | Schema được review |
| A | C | Format bronze, `trade_id`, sample rates | Sample parse được |
| B | D | Bảng Cassandra, cột, alert format | `schema.cql` khớp sink |
| C | D | Gold paths, cột, tần suất, số liệu demo | Local batch chạy được |
| D | A/B/C | Port, env, service name, endpoint | Compose config pass |

### 8.2. Thứ tự tích hợp khuyến nghị

1. Chốt `contracts/` và sample.
2. A dry-run/replay; đảm bảo producer không cần Kafka vẫn kiểm tra được.
3. B test batch logic; chạy Spark nếu có môi trường.
4. C chạy local batch; kiểm tra output gold.
5. D chạy API local với demo fallback, validate Compose/K8s.
6. Nếu đủ tài nguyên, chạy dev stack rồi full stack.
7. Nếu cần gộp code, tạo `integration` từ `main`, merge 4 branch lớn vào đó;
   **không merge vào `main`**.

### 8.3. Kiểm tra tích hợp tối thiểu

```bash
# Trên từng branch lớn
python -m pytest

# Trên branch D
docker compose -f infra/docker-compose.dev.yml config -q
docker compose -f infra/docker-compose.full.yml config -q

# Trên branch C
python batch/run_local_batch.py

# Trên branch D
python -m uvicorn serving.market_api:app --port 8000
curl localhost:8000/health
```

## 9. Checklist nghiệm thu toàn nhóm

- [ ] `main` chỉ có phần nền, không có code A/B/C/D.
- [ ] 4 branch lớn tồn tại và chứa đúng phạm vi.
- [ ] Các branch con đã merge vào branch lớn, không merge vào `main`.
- [ ] `contracts/` khớp với producer, streaming và Cassandra.
- [ ] `make demo` và `make verify` pass.
- [ ] `pytest` pass trên cả 4 branch lớn.
- [ ] Compose dev/full `config -q` pass.
- [ ] K8s YAML và Grafana JSON parse pass.
- [ ] API health/bars/alerts/metrics chạy được với demo fallback.
- [ ] README, runbook, architecture, metrics, report outline đầy đủ.
- [ ] Không commit secret/dữ liệu runtime.
- [ ] Báo cáo ghi rõ giới hạn: chưa chạy full stack, chưa test Spark/Cassandra thật.

## 10. Xử lý sự cố nhanh

| Hiện tượng | Nguyên nhân thường gặp | Cách xử lý |
|---|---|---|
| `ModuleNotFoundError` | Sai branch hoặc thiếu package | `git branch --show-current`; cài đúng requirements |
| `pyspark`/Java lỗi | Máy chưa có Java/Spark | Dùng `make demo`, test logic; chạy Spark trên máy khác |
| Producer dry-run lỗi | Thiếu sample/sai đường dẫn | Kiểm tra `contracts/samples/` |
| Kafka không healthy | Volume KRaft cũ hoặc thiếu dung lượng | Xem log; `down -v` rồi start lại |
| Cassandra chậm | Khởi động 60-90 giây | Chờ healthcheck; chạy `init-cassandra.sh` sau |
| MinIO không tạo bucket | Sai network/credential | Kiểm tra `MINIO_NETWORK`, env |
| API trả demo dù có Cassandra | Cassandra chưa sẵn sàng | Chờ 5 giây để API retry; kiểm tra `/health` |
| Grafana không có bảng | Thiếu Infinity plugin | Dùng Compose/K8s có cài plugin hoặc cài thủ công |
| Hết đĩa khi `docker compose up` | Image quá lớn | Chỉ chạy `config -q`; dọn image/volume |
| Merge conflict | Branch con lệch branch lớn | Merge branch lớn vào branch con, resolve, test, merge lại |

## 11. Mẫu báo cáo tiến độ cho 4 đầu mối

Gửi theo mẫu ngắn:

```text
Đầu mối: A/B/C/D
Branch: feature/...
Đã xong:
- ...
Đang làm:
- ...
Blocker:
- ...
Cần review từ:
- ...
Bằng chứng:
- commit: ...
- lệnh đã chạy: ...
- kết quả test/demo: ...
```

## 12. Lịch gợi ý 3 giờ

| Thời gian | Việc cần làm |
|---|---|
| 0:00-0:20 | Cả nhóm chốt contract, checkout branch, chạy `make demo`/`make test` |
| 0:20-1:30 | 4 đầu mối làm branch con; commit sớm, merge vào branch lớn |
| 1:30-2:15 | Bàn giao A->B/C, B/C->D; D chạy API/demo, validate Compose/K8s |
| 2:15-2:45 | Viết README/report, ghi số liệu demo, chụp màn hình nếu có |
| 2:45-3:00 | Checklist nghiệm thu, `git log --graph`, chốt giới hạn và báo cáo |

## 13. Tài liệu tham chiếu

- `README.md` - tổng quan và lệnh nhanh.
- `docs/branching.md` - bản đồ branch và merge policy.
- `ingest/README.md` - chi tiết vai A.
- `streaming/README.md` - chi tiết vai B.
- `batch/README.md`, `batch/airflow/README.md` - chi tiết vai C.
- `infra/README.md`, `infra/k8s/README.md`, `serving/README.md` - chi tiết vai D.
- `docs/architecture.md`, `docs/runbook.md`, `docs/metrics.md`,
  `docs/report_outline.md` - tài liệu kiến trúc/vận hành/báo cáo.
