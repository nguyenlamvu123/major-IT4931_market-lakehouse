# Kubernetes (vai D)

Triển khai đầy đủ theo PDF *Kiến trúc – Phân công*: minikube trên máy nhóm trưởng (MacBook
M1 Max, khoảng 16 GB cho cụm), các operator Strimzi, K8ssandra, Spark Operator, Airflow,
cùng Prometheus/Grafana. Tên namespace, service, secret và image được chốt trong
`contracts/infra.yaml`.

```bash
make up        # dựng toàn bộ trên máy sạch (infra/scripts/up.sh)
make status    # pod/CR theo namespace
make down      # gỡ toàn bộ
```

## Thứ tự dựng (`infra/scripts/up.sh`)

1. `minikube start --cpus 6 --memory 16g`
2. Helm (phiên bản chốt trong `helm/versions.env`):
   - cert-manager
   - strimzi-kafka-operator (ns `kafka`)
   - k8ssandra-operator (ns `cassandra`, cluster-scoped)
   - spark-operator (ns `spark`)
   - kube-prometheus-stack (ns `monitoring`)
3. `make secrets`: tạo `minio-credentials` và `market-api-keys` từ `.env`.
4. `kustomize build --load-restrictor LoadRestrictionsNone infra/k8s | kubectl apply -f -`
   để tạo các CR (Kafka, KafkaTopic, K8ssandraCluster), MinIO, các Job khởi tạo,
   market-api, monitor và ConfigMap dashboard.
5. Helm Airflow (ns `airflow`) chạy sau, khi Postgres trong `airflow/postgres.yaml` đã lên.
6. Chờ các CR Ready: `kafka/market`, `k8ssandracluster/market`, job `minio-buckets`,
   job `cassandra-schema`.

## Bố cục

| Thư mục | Nội dung |
|---|---|
| `namespaces.yaml` | 7 namespace của hợp đồng |
| `kafka/kafka.yaml` | `KafkaNodePool` + `Kafka` `market` (KRaft, 1 node dev), `kafkaExporter` |
| `kafka/topics.yaml` | **Sinh tự động** bằng `python -m infra.scripts.gen_kafka_topics` từ `contracts/topics.yaml` |
| `minio/minio.yaml` | MinIO ns `minio` + Job tạo bucket `lake`, `checkpoints` |
| `cassandra/` | `K8ssandraCluster` `market` (dc1) + Job áp `contracts/cassandra.cql` (ConfigMap sinh bằng kustomize) |
| `spark/sparkapplication-template.yaml` | Mẫu SparkApplication cho B và C; không apply |
| `airflow/` | Postgres metadata + RBAC cho phép Airflow tạo SparkApplication ở ns `spark` |
| `market/market-api.yaml` | API vai D, `DEMO_FALLBACK=false` |
| `monitoring/monitors.yaml` | ServiceMonitor/PodMonitor: market-api, producer/streaming, kafka-exporter, MinIO |
| `helm/*.yaml` | Values cho từng Helm release, `versions.env` chốt version chart |

## Quy ước cho A, B, C

- Image `btl/<vai>:dev` dùng `imagePullPolicy: IfNotPresent`. `make images` build cho
  `linux/arm64` rồi chạy `minikube image load`.
- Kafka: `market-kafka-bootstrap.kafka.svc:9092`. MinIO: `http://minio.minio.svc:9000`.
  Cassandra: `market-dc1-service.cassandra.svc:9042`.
- SparkApplication đặt ở ns `spark` với `serviceAccount: spark-operator-spark`. Copy từ
  `spark/sparkapplication-template.yaml`.
- Để Prometheus thu metrics, pod producer/streaming cần label `btl.market/metrics: "true"`
  và một container port tên `metrics` (producer 8001, driver streaming 9108).
- Secret `minio-credentials` (key `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`) có sẵn ở các
  ns `minio`, `spark`, `airflow`, `market`. Secret `market-api-keys` có ở ns `market`.

## Kiểm tra không cần cụm

```bash
python -m pytest infra/tests
python -m infra.scripts.gen_kafka_topics --check
kustomize build --load-restrictor LoadRestrictionsNone infra/k8s > /tmp/rendered.yaml
kubeconform -strict -summary -ignore-missing-schemas \
  -schema-location default \
  -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' \
  /tmp/rendered.yaml infra/k8s/spark/sparkapplication-template.yaml
```

`-ignore-missing-schemas` là cần thiết vì catalog CRD chưa có schema cho
`K8ssandraCluster`. Strimzi, SparkApplication và ServiceMonitor/PodMonitor đều được
kiểm tra đầy đủ.

## Demo 3 broker (slide "Hạ tầng" của PDF ý tưởng)

1. Trong `kafka/kafka.yaml`, đặt `KafkaNodePool.spec.replicas: 3`, đổi các
   `*.replication.factor` thành 3 và `min.insync.replicas` thành 2.
2. Sinh lại topic:
   `python -m infra.scripts.gen_kafka_topics --replicas 3 --min-insync 2`.
3. Với K8ssandra 3 node, đặt `size: 3` và `REPLICATION=3` trong `cassandra/schema-job.yaml`.

## Giới hạn

- Cấu hình dev chỉ có 1 broker và 1 node Cassandra, chưa bật auth và TLS. Muốn bật thì xem
  `docs/lessons/` (mảng 10: bảo mật).
- Grafana dùng Infinity datasource đọc market-api. Lý do: Cassandra không có datasource
  Grafana chính thức.
