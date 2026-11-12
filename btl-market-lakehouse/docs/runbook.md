# Runbook

## 1. Demo local (không cần Docker)

```bash
cd btl-market-lakehouse
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
make samples demo verify test
make api        # http://localhost:8000/health -> "source": "demo"
```

## 2. Compose dev: Kafka + MinIO + Cassandra (laptop của A/B/C)

```bash
cp .env.example .env
make dev-up       # bucket lake/checkpoints và schema contracts/cassandra.cql được tạo tự động
make topics       # topic từ contracts/topics.yaml (cần python3 + PyYAML trên máy)
docker compose -f infra/docker-compose.dev.yml ps
```

| Dịch vụ | Từ laptop | Từ container khác |
|---|---|---|
| Kafka | `localhost:9092` | `kafka:29092` |
| MinIO | `http://localhost:9000` (console 9001, minioadmin/minioadmin) | `http://minio:9000` |
| Cassandra | `localhost:9042` | `cassandra:9042` |

`make dev-down` xoá cả volume. Kafka dev không giữ dữ liệu giữa các lần chạy, nên phải
chạy lại `make topics`.

## 3. Kubernetes (cụm chính)

```bash
cp .env.example .env    # điền API key, MINIO_ACCESS_KEY/MINIO_SECRET_KEY
make up                 # khoảng 15–25 phút lần đầu (kéo image, chart)
make port-forward       # Grafana :3000, API :8000, Airflow :8080, Prometheus :9090, MinIO :9001
REQUIRE_REAL_DATA=1 bash scripts/smoke_test.sh
```

Trên một checkout tích hợp (đã có `ingest/`, `streaming/`, `batch/` của các vai),
`make up` cũng build image và apply `ingest/k8s`, `streaming/k8s`. Trên nhánh riêng của
D, bước đó bị bỏ qua và có cảnh báo.

Các lệnh lẻ:

| Lệnh | Làm gì |
|---|---|
| `make images` | Build 6 image `btl/*:dev` cho linux/arm64 rồi `minikube image load` |
| `make secrets` | Tạo Secret từ `.env` |
| `make apply` | Apply lại manifest nền (kustomize) |
| `make apps` | Apply lại workload của A/B |
| `make status` | Pod và CR theo namespace |
| `make down` | Xoá minikube (`SOFT=1`: chỉ gỡ release và namespace) |

## 4. Kiểm tra sức khoẻ

| Kiểm tra | Lệnh | Kỳ vọng |
|---|---|---|
| API | `curl -i localhost:8000/health` | `"status": "ok"`; header `X-Data-Source: cassandra` trên cụm |
| Kafka | `kubectl -n kafka get kafka market` | `READY True` |
| Topic | `kubectl -n kafka get kafkatopics` | 8 topic, đều Ready |
| Cassandra | `kubectl -n cassandra get pods` và job `cassandra-schema` Complete | Bảng của `contracts/cassandra.cql` đã tạo |
| MinIO | job `minio-buckets` Complete | Có bucket `lake`, `checkpoints` |
| Spark | `kubectl -n spark get sparkapplications` | Speed layer `RUNNING` |
| Airflow | http://localhost:8080 (admin/admin) | DAG của C hiện, không có lỗi import |
| Grafana | http://localhost:3000 (admin/admin), thư mục "Market Lakehouse" | Ô "Nguồn dữ liệu" = cassandra |

## 5. Sự cố thường gặp

| Hiện tượng | Nguyên nhân | Xử lý |
|---|---|---|
| Pod `btl/*` ở trạng thái `ErrImageNeverPull` / `ImagePullBackOff` | Chưa nạp image vào minikube | `make images`, rồi xoá pod |
| `kustomize build` báo `security; file ... is not in or below` | Thiếu `--load-restrictor LoadRestrictionsNone` | Dùng `make apply` / `make up`, không dùng `kubectl apply -k` |
| Producer báo `UNKNOWN_TOPIC_OR_PART` | Đã tắt auto-create topic; topic chưa tạo hoặc sai tên | `make topics` (compose) hoặc `kubectl -n kafka get kafkatopics`; kiểm tra tên theo `contracts/topics.yaml` |
| Dashboard: Nguồn dữ liệu = none, panel lỗi 503 | Cassandra mất kết nối (K8s không dùng dữ liệu demo) | `kubectl -n cassandra get pods`; API tự kết nối lại sau tối đa 5 giây |
| Dashboard: Nguồn dữ liệu = demo trên cụm | Manifest bị sửa `DEMO_FALLBACK` | Đặt lại `false` trong `infra/k8s/market/market-api.yaml` |
| Operator khởi động lại liên tục sau khi máy ngủ | Mất leader lease (đã gặp, slide "Sự cố về hệ thống") | Giữ máy thức (`caffeinate -dims`); operator tự hồi phục |
| Panel Kafka lag trống | Chưa có connector, hoặc kafka-exporter chưa được scrape | `kubectl -n kafka get pods -l strimzi.io/name=market-kafka-exporter`; xem target trên Prometheus |
| Panel Spark/producer trống | Pod thiếu label `btl.market/metrics=true` hoặc port tên `metrics` | Sửa manifest của vai A/B theo `infra/k8s/README.md` |
| Cassandra lâu Ready | Lần đầu cần vài phút | `make status`; job schema tự chờ |

## 6. Thử lỗi

Xem `infra/fault_injection/README.md`. Kết quả ghi vào `docs/lessons/D-fault-injection.md`.

## 7. Demo

Xem `docs/demo_script.md`.
