# Kubernetes (role D)

Manifests target a single-node/dev cluster (Docker Desktop, kind, minikube) with a
default StorageClass. For production, replace `SimpleStrategy` Cassandra replication,
use a real Kafka cluster, and add resource quotas.

## Apply

```bash
kubectl apply -k infra/k8s
kubectl -n market get pods
```

Order is handled by kustomize; Cassandra's init Job retries until the node is ready.

## Images

Build the two custom images from the integrated repo (or from the relevant branches):

```bash
docker build -f serving/Dockerfile -t btl/market-api:latest ./serving
docker build -f infra/k8s/airflow/Dockerfile -t btl/airflow:latest .
```

`btl/airflow:latest` must contain `batch/dags` and the batch jobs at `/opt/app`. The
manifest assumes `/opt/app` paths for `MARKET_PROJECT_DIR`.

## Access

```bash
kubectl -n market port-forward svc/grafana 3000:3000
kubectl -n market port-forward svc/market-api 8000:8000
kubectl -n market port-forward svc/airflow 8080:8080
kubectl -n market port-forward svc/spark-master 8080:8080
```

## Resource notes

The full manifest is sized for ~8-10 GB RAM across Kafka, MinIO, Cassandra, Spark,
Airflow, Prometheus and Grafana. It will not fit comfortably on the current 7.6 GB
machine together with the desktop; use the dev Compose stack for quick tests.

## Validate without a cluster

```bash
python - <<'PY'
import glob, yaml
for path in glob.glob("infra/k8s/*.yaml"):
    list(yaml.safe_load_all(open(path)))
    print("ok", path)
PY
```

## Known gaps

- Kafka is a single KRaft broker; no replication.
- Cassandra is a single node with RF=1.
- Airflow uses `LocalExecutor` and `standalone`; for production use Celery/Kubernetes
  executor and a real metadata database.
- Spark master/worker are stateless; submit jobs with `spark-submit --master
  spark://spark-master:7077`.
