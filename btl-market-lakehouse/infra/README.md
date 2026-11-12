# Hạ tầng (vai D)

Vai D "dựng toà nhà để code của mọi người chạy được, rồi đặt bảng điện tử ở sảnh để giám
khảo xem" (PDF mục 5). Tên namespace, service, secret, image và cổng metric được chốt
trong `contracts/infra.yaml`.

## Chọn môi trường

| Môi trường | Lệnh | RAM | Dùng khi | Không dùng khi |
|---|---|---|---|---|
| Compose dev: Kafka + MinIO + Cassandra | `make dev-up && make topics` | khoảng 6 GB | A/B/C phát triển trên laptop với Spark local (PDF việc D.4) | Cần Airflow, Grafana hoặc operator |
| Compose full | `make images-local && docker compose -f infra/docker-compose.full.yml up -d && make topics` | 12–16 GB | Máy không chạy được minikube nhưng cần dashboard | Demo chính thức (DAG cần K8s) |
| Kubernetes | `make up` | minikube 16 GB / 6 CPU | Cụm chính, demo, đo số liệu, thử lỗi | Laptop dưới 16 GB RAM |

## Thư mục

| Đường dẫn | Nội dung |
|---|---|
| `docker-compose.dev.yml` | Kafka 3.9 (`localhost:9092` / `kafka:29092`), MinIO + tạo bucket, Cassandra + áp `contracts/cassandra.cql` |
| `docker-compose.full.yml` | Gồm file dev, cộng thêm Kafka Connect (`btl/kafka-connect-s3:dev`), kafka-exporter, Spark standalone, Airflow, market-api, Prometheus, Grafana |
| `k8s/` | Operator CR, manifest và Helm values. Xem `k8s/README.md` |
| `airflow/Dockerfile` | Image `btl/airflow:dev` (DAG và template của C) |
| `scripts/` | `up.sh`, `down.sh`, `images.sh`, `secrets.sh`, `status.sh`, `port-forward.sh`; `init-*.sh` cho compose; `topics_contract.py`, `gen_kafka_topics.py`, `build_dashboards.py` |
| `monitoring/` | `prometheus.yml` (compose), Grafana provisioning, 2 dashboard sinh từ code |
| `fault_injection/` | Kịch bản thử lỗi (K8s và compose) |
| `tests/` | Test tĩnh: compose, manifest K8s, script, dashboard, cấu hình GitHub, thử lỗi |

## Nguyên tắc

- **Một nguồn sự thật:**
  - Topic sinh từ `contracts/topics.yaml` (compose: `init-topics.sh`; K8s: `KafkaTopic`
    do `gen_kafka_topics.py` sinh).
  - Schema Cassandra đọc thẳng từ `contracts/cassandra.cql` (compose: mount file; K8s:
    kustomize `configMapGenerator`).
  - Trong `infra/` không còn bản sao CQL hay danh sách topic nào; có test chặn việc này.
- **Image:** không dùng `bitnami/*` (Bitnami đã rút catalog miễn phí năm 2025). Image
  `btl/*:dev` được build cho linux/arm64 và nạp vào minikube (`IfNotPresent`).
- **Bí mật:** chỉ nằm trong `.env`. `make secrets` tạo Secret K8s; manifest không chứa key.
- **Dữ liệu demo:** chỉ dùng trên laptop. Trên cụm, `DEMO_FALLBACK=false` nên market-api
  trả 503 khi Cassandra hỏng.

## Kiểm tra

```bash
python -m pytest infra/tests
make k8s-validate     # topic/dashboard sinh tự động còn khớp + kustomize + kubeconform
```

CI chạy các bước trên ở job `infra` (`.github/workflows/ci.yml` tại gốc repo).

## Giới hạn

- Cấu hình dev có 1 broker và 1 node Cassandra, chưa bật auth hay TLS. Cách nâng lên 3
  broker / 3 node có trong `k8s/README.md`.
- Workload của A và B (`ingest/k8s`, `streaming/k8s`) nằm trên nhánh của từng vai.
  `make up` chỉ apply chúng trên một checkout tích hợp.
- Máy phát triển của D không có Docker hay kubectl: compose và manifest mới được kiểm tra
  tĩnh (parse, kustomize, kubeconform), chưa chạy thật trên cụm.
