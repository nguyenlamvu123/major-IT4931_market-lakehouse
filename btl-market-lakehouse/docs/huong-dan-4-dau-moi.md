# Hướng dẫn điều phối 4 đầu mối A/B/C/D

> Tài liệu cho trưởng nhóm và 4 đầu mối vai A, B, C, D. Đọc xong, mỗi người biết: mình sở
> hữu gì, làm trên branch nào, kiểm tra bằng lệnh gì, bàn giao cho ai, và không phá quy ước
> chung của repo.
>
> Cập nhật ngày 12/11/2026, theo **hợp đồng v2** bám sát PDF *BTL-IT4931 Kiến trúc – Phân
> công* (09/11/2026). Mọi thay đổi so với bản trước, danh sách nhánh con và kết quả kiểm thử
> nằm trong `docs/thay-doi-theo-pdf.md` (có trên `main` và cả 4 nhánh vai).

## 0. Nguyên tắc bất di bất dịch

1. `main` chỉ giữ phần nền dùng chung: `contracts/` (hợp đồng v2), `common/`, `scripts/`,
   `tests/`, `docs/`, `README.md`, `Makefile`, cấu hình Python.
2. Có **4 branch lớn**, mỗi vai sở hữu một: `feature/A-ingest`, `feature/B-streaming`,
   `feature/C-batch`, `feature/D-infra`.
3. Mỗi việc là **một branch con** tạo từ branch lớn của vai, xong thì merge `--no-ff` trở lại
   branch lớn đó. **Không merge branch vai vào `main`**, và không merge branch vai này vào
   branch vai khác.
4. Thay đổi dùng chung chỉ đi một chiều: `main` → 4 branch lớn (`git merge main`). Muốn
   tích hợp cả 4 vai thì tạo riêng branch `integration`.
5. `contracts/` (trên `main`) là nguồn sự thật cho topic, schema, sample, đường dẫn MinIO,
   bảng Cassandra và tên hạ tầng. Đổi hợp đồng phải được vai nhận dữ liệu review, và
   `tests/test_contracts.py` phải xanh.
6. Không commit `.env`, API key, `data/`, checkpoint, log hay file lớn.
7. Quy ước tên nhánh và PR vào `main` trong PDF (mục 7–8) **đang tạm hoãn**. Nhóm sẽ quyết
   lại khi tích hợp.

## 1. Bản đồ branch và quyền sở hữu

| Vai | Branch lớn | Thư mục sở hữu | Bàn giao chính |
|---|---|---|---|
| A | `feature/A-ingest` | `ingest/` (và `contracts/` trên `main` theo CODEOWNERS) | Dữ liệu trong 7 topic cho B, C; bronze trên MinIO cho C |
| B | `feature/B-streaming` | `streaming/` | Bảng realtime trong Cassandra và topic `alerts` cho D |
| C | `feature/C-batch` | `batch/` | silver/gold trên MinIO, bảng batch trong Cassandra, kết quả ML cho D |
| D | `feature/D-infra` | `infra/`, `serving/`, `.github/` (ở gốc git), `docs/` chung | Cụm K8s (`make up`), Compose dev, market-api, dashboard, CI |

Các branch con của từng vai, cùng nội dung của chúng, liệt kê ở `docs/thay-doi-theo-pdf.md`
mục 3.

### 1.1. Luồng dữ liệu (hợp đồng v2)

```text
Binance ─┐                      ┌─ crypto.trades ─┐
Alpaca  ─┤                      ├─ stocks.trades ─┤      B: Spark Streaming (update, 5 s)
Finnhub ─┼─ A: producers ─Kafka─┼─ stocks.bars    ├────► ohlcv_1m, ohlcv_5m, indicators_latest,
Yahoo   ─┤   (key = symbol)     ├─ gold.quotes    │      alerts_by_day  + topic alerts
FRED/   ─┘                      ├─ fx.dxy ────────┘
Treasury                        └─ rates.yields (key = tenor)
                                       │ A: Kafka Connect S3 sink
                                       ▼
            MinIO s3a://lake/bronze/<topic>/dt=YYYY-MM-DD/
                                       │ C: bronze→silver → kiểm tra chất lượng → silver→gold
                                       ▼              (Airflow 01:00 UTC)
            silver/{trades,bars,yields,alerts}, gold/{daily_summary,correlation_matrix,
            yield_curve,ml_features,asset_clusters}  ──► Cassandra (bảng batch)
                                       │
                 D: market-api ──► Grafana (dashboard thị trường)
                 Prometheus  ──► Grafana (dashboard giám sát: Kafka lag, Spark, CPU/RAM)
```

Cassandra là nơi hai luồng gặp nhau: B ghi bảng realtime, C ghi bảng batch, D đọc cả hai.

### 1.2. Trạng thái hiện tại (12/11/2026, tuần 1)

- Hợp đồng v2 đã chốt và nằm trên `main`, đã merge vào cả 4 branch lớn.
- 4 branch lớn đều có code theo hợp đồng v2 và test xanh: A 73, B 60, C 59, D 127 test.
  `main` có 26 test.
- **Chưa chạy trên cụm hay Docker thật.** Danh sách việc cần cụm thật và các rủi ro nằm ở
  `docs/thay-doi-theo-pdf.md`, mục 7–8.

## 2. Chuẩn bị chung cho cả 4 đầu mối

### 2.1. Cài môi trường

```bash
cd btl-market-lakehouse
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env            # điền API key của bạn; không bao giờ commit .env
```

- Nếu Ubuntu báo thiếu `ensurepip`: `sudo apt install python3-venv`, hoặc tạo venv bằng
  `python3 -m venv --without-pip .venv` rồi cài pip bằng `get-pip.py`.
- Test Spark của B, C cần **Java 17**: `export JAVA_HOME=...`. Không có Java thì các test
  này tự bỏ qua.
- Đặt git bằng tên và email GitHub của chính bạn (PDF mục 7.2), để lịch sử commit thể hiện
  đúng ai làm phần nào.

### 2.2. Kiểm tra phần nền dùng chung

```bash
python scripts/generate_samples.py   # 20 dòng mẫu cho mỗi topic trong contracts/samples
make demo                            # pandas, không cần Kafka/Spark/Cassandra
make verify
make test
```

`data/demo/summary.json` trả lời 3 câu hỏi trong slide "So sánh với trái phiếu" trên dữ
liệu giả lập: `corr_TLT_US10Y`, `corr_GLD_REAL10Y`, `corr_SPY_TLT`.

## 3. Quy trình branch, commit và merge

```bash
git checkout feature/B-streaming
git merge main                                   # nhận hợp đồng/common mới nhất
git checkout -b feature/B-<việc>                 # một việc = một branch con
# sửa code, chạy test
git add <file>
git commit -m "feat(B-<việc>): <làm gì>"         # Conventional Commits
git checkout feature/B-streaming
git merge --no-ff feature/B-<việc> -m "merge(B): <mô tả>"
```

Trước khi bàn giao:

```bash
python -m pytest -q
python -m ruff check .
```

Quy tắc review:

- A đổi hợp đồng thì B hoặc C phải review.
- B/C đổi cột ghi Cassandra thì phải khớp `contracts/cassandra.cql`; test dùng
  `common.cql.load_tables()` sẽ bắt chỗ lệch.
- C đổi đường dẫn silver/gold thì sửa `contracts/paths.yaml` và báo D.
- D đổi namespace, service, secret hay cổng thì sửa `contracts/infra.yaml` và báo A/B/C.
- Không force-push branch đã chia sẻ.

## 4. Đầu mối A: Thu thập dữ liệu và Kafka

**Một câu (PDF):** giá từ 4 nguồn chảy vào Kafka liên tục, đúng định dạng, kể cả khi sàn Mỹ
đóng cửa.

| File | Vai trò |
|---|---|
| `ingest/run_producer.py` | Entrypoint: `--source binance\|alpaca\|finnhub\|yahoo\|fred\|treasury\|mock\|replay\|backfill` |
| `ingest/producers/` | Binance WS, Alpaca IEX WS (trade + nến 1Min), Finnhub WS, Yahoo (GC=F, DXY), FRED, Treasury CSV, mock, replay |
| `ingest/backfill_yfinance.py` | Nến ngày nhiều năm của mọi mã, đẩy vào `stocks.bars` |
| `ingest/kafka_connect/` | S3 sink sang bronze (2 connector: dữ liệu và alerts), Dockerfile `btl/kafka-connect-s3` |
| `ingest/k8s/` | Deployment producer (ns `market`), KafkaConnect/KafkaConnector (ns `kafka`), ServiceMonitor |
| `ingest/tools/` | `topic_rates.py` (tin/giây mỗi topic), `feed_quality.py` (% trùng/muộn), `bench_publish.py` |

```bash
python -m ingest.run_producer --source mock --rows 50 --dry-run --metrics-port 0
python -m ingest.run_producer --source replay --file contracts/samples --speed 100 --dry-run
export KAFKA_BOOTSTRAP=localhost:9092
python -m ingest.run_producer --source binance
python -m ingest.run_producer --source treasury --interval 3600
python -m ingest.backfill_yfinance --years 3
python -m pytest ingest/tests -q
```

**Xong khi (PDF):** 4 nguồn chạy liên tục 1 giờ trên K8s không lỗi, và có số tin nhắn/giây
cho từng topic. Ghi kết quả vào `docs/metrics/A-ingest.md`.

**Bàn giao:**

- Cho B và C: 7 topic theo hợp đồng, khóa `symbol` (riêng `rates.yields` dùng `tenor`),
  `trade_id` tất định, mẫu trong `contracts/samples/`.
- Cho C: bronze theo `contracts/paths.yaml`.
- Cho D: tên Deployment, cổng metrics 8001, secret `market-api-keys`.

**Rủi ro:**

- Giới hạn tần suất API. Đã có backoff, circuit breaker, và các nguồn không cần key.
- Feed IEX miễn phí chỉ chiếm một phần khối lượng toàn thị trường.

## 5. Đầu mối B: Speed layer

**Một câu (PDF):** biến dòng giao dịch rời rạc thành nến giá, chỉ báo và cảnh báo trong
vài giây.

| File | Vai trò |
|---|---|
| `streaming/job.py` | 2 query: `speed-ohlcv-1m`, `speed-ohlcv-5m`. Output mode update, trigger 5 s, watermark 30 s, bỏ trùng `trade_id`, nguồn Kafka hoặc `--source files` |
| `streaming/state.py` | `IndicatorState` giữ 60 nến/mã qua các batch, nạp lại từ Cassandra khi khởi động lại; tính chỉ báo và cảnh báo |
| `streaming/sinks.py`, `columns.py` | Ghi `ohlcv_1m`, `ohlcv_5m`, `indicators_latest`, `alerts_by_day` đúng cột hợp đồng; ghi topic `alerts` |
| `streaming/metrics.py` | Gauge Prometheus trên cổng 9108 |
| `streaming/tools/` | `measure_latency.py`, `exactly_once_check.py`, `local_latency_run.py` |
| `streaming/k8s/speed-layer.yaml` | SparkApplication (ns `spark`), Service, PodMonitor |

```bash
spark-submit streaming/job.py --source files --input contracts/samples --available-now \
  --output-mode parquet --parquet-path /tmp/speed --checkpoint-root /tmp/checkpoints --metrics-port 0
JAVA_HOME=/path/to/jdk17 python -m pytest streaming/tests -q
python -m streaming.tools.local_latency_run --seconds 60
```

**Xong khi (PDF):** nến 1 phút có trong Cassandra chậm dưới khoảng 10 giây so với thời điểm
giao dịch.

- Đo local: p95 2,1 s.
- Còn phải đo trên cụm bằng `measure_latency.py`, và chứng minh exactly-once bằng cách xoá
  pod driver (`infra/fault_injection/k8s/kill_spark_driver.sh`).

## 6. Đầu mối C: Batch layer và Data Lake

**Một câu (PDF):** giữ kho lịch sử gọn gàng, và mỗi đêm tính ra những con số chính xác,
sâu hơn luồng realtime.

| File | Vai trò |
|---|---|
| `batch/jobs/bronze_to_silver.py` | Đọc từng `<topic>/dt=`; làm sạch, bỏ trùng, chuẩn UTC; ghi silver trades/bars/yields/alerts |
| `batch/jobs/quality_checks.py` | Cổng chất lượng giữa silver và gold: báo cáo `gold/_quality`, exit 1 khi có lỗi |
| `batch/gold.py`, `batch/jobs/silver_to_gold.py` | daily_summary (ưu tiên nến ngày > trades > nến phút), tương quan 90 ngày với US2Y/US10Y/REAL10Y theo lịch SPY + forward-fill, yield curve |
| `batch/jobs/cassandra_sink.py` | Ghi `daily_summary`, `correlation_matrix`, `yield_curve`, `asset_clusters` |
| `batch/jobs/train_kmeans.py` | KMeans phân cụm tài sản, tính silhouette |
| `batch/jobs/benchmark.py` | Số đo trước/sau: AQE, broadcast, partition, cache, pruning |
| `batch/dags/`, `batch/k8s/` | DAG `0 1 * * *` dùng `SparkKubernetesOperator` + 5 template SparkApplication |

```bash
python batch/run_local_batch.py
export LAKE=file:///tmp/lake
python batch/jobs/bronze_to_silver.py --lake $LAKE --start 2026-09-01 --end 2026-11-29 --master 'local[2]'
python batch/jobs/quality_checks.py   --lake $LAKE --dt 2026-11-25 --master 'local[2]'
python batch/jobs/silver_to_gold.py   --lake $LAKE --dt 2026-11-25 --master 'local[2]'
JAVA_HOME=/path/to/jdk17 python -m pytest batch/tests -q
```

**Xong khi (PDF):** DAG tự chạy 3 đêm liên tiếp thành công, và có bảng số liệu tối ưu
trước/sau. Số đo local đã có trong `docs/metrics/C-batch.md`; số trên cụm vẫn còn thiếu.

## 7. Đầu mối D: Hạ tầng, API, Dashboard, CI

**Một câu (PDF):** dựng "toà nhà" để code của mọi người chạy được, rồi đặt bảng điện tử ở
sảnh cho giám khảo xem.

| File | Vai trò |
|---|---|
| `infra/docker-compose.dev.yml` | Kafka (`apache/kafka`, `localhost:9092`) + MinIO + Cassandra. Tự tạo bucket và schema |
| `infra/k8s/` | Strimzi, MinIO, K8ssandra, Spark Operator, Airflow (KubernetesExecutor), kube-prometheus-stack, market-api |
| `infra/scripts/` | `up.sh`, `down.sh`, `images.sh`, `secrets.sh`, `gen_kafka_topics.py`, `build_dashboards.py` |
| `serving/` | market-api: `/api/v1/ohlcv`, `indicators/latest`, `alerts`, `daily`, `daily/compare`, `correlation`, `yield-curve`, `clusters`, `symbols` |
| `infra/monitoring/` | Dashboard thị trường + dashboard giám sát |
| `infra/fault_injection/k8s/` | 6 kịch bản thử lỗi |
| `.github/` (gốc git) | CI (ruff + pytest + kubeconform), CODEOWNERS, mẫu PR |

```bash
make dev-up && make topics          # máy yếu: Compose dev
make images && make secrets         # build linux/arm64 + minikube image load; secret lấy từ .env
make up                             # dựng toàn bộ trên minikube (~16 GB RAM)
make port-forward                   # Grafana :3000, API :8000, Airflow :8080, Prometheus :9090
REQUIRE_REAL_DATA=1 bash scripts/smoke_test.sh
make down
```

**Xong khi (PDF):** một máy sạch chạy `make up` là lên đủ hệ thống, và dashboard có số liệu.

Ngoài ra trên Kubernetes, market-api đặt `DEMO_FALLBACK=false`. Khi Cassandra không trả lời,
API trả 503 thay vì âm thầm hiện dữ liệu giả.

## 8. Bàn giao theo cặp

| Từ | Đến | Artifact | Điều kiện |
|---|---|---|---|
| A | B, C | 7 topic, schema, 20 mẫu/topic | `tests/test_contracts.py` xanh |
| A | C | Bronze `s3a://lake/bronze/<topic>/dt=…/` | S3 sink chạy, có file trong MinIO |
| B | D | `ohlcv_1m`, `ohlcv_5m`, `indicators_latest`, `alerts_by_day`, topic `alerts` | Test cột so với `cassandra.cql` xanh |
| C | D | `daily_summary`, `correlation_matrix`, `yield_curve`, `asset_clusters` | Test cột so với `cassandra.cql` xanh |
| D | A, B, C | Namespace, service, secret, image, cổng metrics (`contracts/infra.yaml`) | `make up` chạy được |

## 9. Checklist nghiệm thu toàn nhóm

- [ ] `contracts/` trên `main` khớp producer, streaming, batch và market-api (test xanh trên 4 branch).
- [ ] `make demo`, `make verify`, `make test` xanh trên `main`.
- [ ] `python -m pytest -q` xanh trên cả 4 branch lớn (B, C chạy cùng Java 17).
- [ ] `make up` trên máy sạch; dashboard thị trường và dashboard giám sát có số liệu.
- [ ] 4 nguồn chạy 1 giờ; độ trễ nến dưới ~10 s; DAG chạy 3 đêm; 6 kịch bản thử lỗi đã ghi.
- [ ] `docs/metrics.md` và `docs/lessons/` có số đo thật, ghi ngay khi đo.
- [ ] CODEOWNERS đã điền username thật; bật bảo vệ nhánh trên GitHub.
- [ ] Không commit secret hay dữ liệu runtime.

## 10. Xử lý sự cố nhanh

| Hiện tượng | Nguyên nhân thường gặp | Cách xử lý |
|---|---|---|
| `ensurepip is not available` | Ubuntu thiếu `python3-venv` | `sudo apt install python3-venv` hoặc `--without-pip` + `get-pip.py` |
| `ModuleNotFoundError: distutils` khi gọi `toPandas()` | pyspark 3.5 chạy trên Python 3.12 | `pip install setuptools` (đã có trong `requirements.txt`) |
| Test Spark bị skip | Thiếu Java | Đặt `JAVA_HOME` trỏ JDK 17 |
| `tests/test_contracts.py` fail | Sửa `contracts/` mà không sửa `common/contracts.py` (hoặc ngược lại) | Sửa cả hai trong cùng một PR |
| Kafka trong Compose không lên | Volume KRaft cũ | `make dev-down` (xoá volume) rồi `make dev-up` |
| market-api trả 503 | Cassandra chưa sẵn sàng và `DEMO_FALLBACK=false` | Kiểm tra `/health`, pod Cassandra; ở local có thể đặt `DEMO_FALLBACK=true` |
| Grafana không có bảng thị trường | Thiếu plugin Infinity | Values của kube-prometheus-stack đã cài plugin; chạy ngoài cụm thì cài tay |
| Merge conflict | Branch con lệch branch lớn | Merge branch lớn vào branch con, giải quyết conflict, chạy test, rồi merge lại |

## 11. Mẫu báo cáo tiến độ

```text
Đầu mối: A/B/C/D
Branch: feature/...
Đã xong:
Đang làm:
Blocker:
Cần review từ:
Bằng chứng: commit, lệnh đã chạy, kết quả test/demo, số đo (đã ghi vào docs/metrics...)
```

## 12. Kế hoạch 6 tuần (theo PDF, tính từ 09/11, xong hệ thống trước 20/12)

| Tuần | A | B | C | D | Mốc cả nhóm |
|---|---|---|---|---|---|
| 1 (09–15/11) | Schema + mẫu ✅; producer Binance gửi Kafka | Spark local đọc file mẫu ra nến 1 phút ✅ | Thiết kế bronze/silver/gold ✅ | Repo; Kafka + MinIO; Compose ✅ | Chốt `contracts/` ✅ |
| 2 (16–22/11) | Producer cổ phiếu, vàng, Treasury chạy thật | Đọc Kafka thật, ghi Cassandra | bronze→silver trên MinIO | Cassandra + Spark Operator | 1 nguồn chạy hết luồng nhanh |
| 3 (23–29/11) | Kafka Connect → MinIO; replay | VWAP, biến động, cảnh báo trên dữ liệu thật | gold: join đa tài sản, tương quan | Prometheus/Grafana, dashboard giám sát | Cả 2 luồng có dữ liệu |
| 4 (30/11–06/12) | Chạy liên tục 24 giờ | Exactly-once, đo độ trễ | Airflow, MLlib | Dashboard thị trường | Ngừng thêm tính năng |
| 5 (07–13/12) | Đo throughput | Thử xoá pod, dữ liệu đến muộn | Tối ưu trước/sau trên cụm | `make up`, thử lỗi | Bản nháp báo cáo |
| 6 (14–20/12) | Slide + chương A | Slide + chương B | Slide + chương C | Ghép báo cáo, video demo | Tập thuyết trình 2 lần |

Dấu ✅ nghĩa là đã có code và test; các việc "chạy thật" vẫn cần cụm.

## 13. Tài liệu tham chiếu

- `docs/thay-doi-theo-pdf.md`: toàn bộ thay đổi theo PDF, danh sách nhánh, lỗi đã sửa, việc còn lại.
- `contracts/README.md`: hợp đồng v2.
- `README.md`, `docs/branching.md`.
- `ingest/README.md` (A), `streaming/README.md` (B), `batch/README.md` + `batch/airflow/README.md` (C).
- `infra/README.md`, `infra/k8s/README.md`, `serving/README.md`, `docs/architecture.md`,
  `docs/runbook.md`, `docs/metrics.md`, `docs/report_outline.md`, `docs/demo_script.md` (D).
