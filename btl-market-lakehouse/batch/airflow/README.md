# Airflow – DAG `market_lakehouse_batch` (vai C)

```text
01:00 UTC mỗi ngày, dt = {{ ds }} (ngày UTC hôm trước)
bronze_to_silver → quality_checks → silver_to_gold → cassandra_sink → train_kmeans
```

Mỗi task là một `SparkKubernetesOperator`. Operator này render template
`batch/k8s/<task>.yaml` bằng Jinja (`{{ ds }}`, `{{ ds_nodash }}`), tạo SparkApplication
trong namespace `spark`, chờ chạy xong, lấy log driver về Airflow rồi xoá đối tượng.

## Vì sao 01:00 UTC

- 01:00 UTC là 08:00 giờ Việt Nam, sau khi phiên Mỹ đóng cửa (04:00 VN) bốn tiếng.
- Lần chạy cho ngày `ds` = D bắt đầu lúc 01:00 UTC ngày D+1, khi toàn bộ ngày UTC D đã có
  trong bronze (Kafka Connect flush mỗi phút).
- Lợi suất của ngày D có thể chưa được công bố lúc đó. Các job đọc lại 7 ngày
  `rates.yields` gần nhất, nên lần chạy sau sẽ tự bổ sung.

## Thứ tự và cách xử lý lỗi

| Task | Nếu lỗi |
|---|---|
| bronze_to_silver | Spark Operator thử lại 2 lần (`OnFailure`), sau đó Airflow thử lại 2 lần cách nhau 10 phút |
| quality_checks | `restartPolicy: Never`: một luật hỏng sẽ hỏng lại y hệt, DAG dừng trước khi gold bị ghi đè |
| silver_to_gold, cassandra_sink, train_kmeans | như bronze_to_silver; chạy lại là idempotent (ghi đè đúng phân vùng, upsert theo khóa chính) |

`catchup=True` từ `2026-11-09` cùng với `max_active_runs=1`: Airflow chạy bù lần lượt từng
ngày bị lỡ, không chạy song song.

## Triển khai (D dựng Airflow, C giao DAG + template)

Image Airflow (`btl/airflow:dev`, do D build) cần có:

1. Provider `apache-airflow-providers-cncf-kubernetes` (đã đi kèm image chính thức).
2. File `batch/dags/market_lakehouse_dag.py` trong thư mục DAG. File này không import gì
   từ `batch`, nên chép một mình nó là đủ.
3. Thư mục `batch/k8s/`, và biến `MARKET_BATCH_K8S_DIR` trỏ tới nó, ví dụ:

```dockerfile
COPY batch/dags/market_lakehouse_dag.py /opt/airflow/dags/
COPY batch/k8s /opt/airflow/batch-k8s
ENV MARKET_BATCH_K8S_DIR=/opt/airflow/batch-k8s
```

Trên cụm cần thêm:

- Connection `kubernetes_default` (in-cluster) và RBAC cho ServiceAccount của worker
  Airflow: được `create/get/list/watch/delete` `sparkapplications.sparkoperator.k8s.io`,
  được `get/list` `pods` và `pods/log` trong namespace `spark`.
- Trong namespace `spark`:
  - secret `minio-credentials` (`MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`);
  - secret `cassandra-credentials` (`username`, `password`), chép từ secret
    `market-superuser` của K8ssandra; bỏ qua được nếu Cassandra không bật auth.
- Spark Operator bật webhook (`webhook.enable=true`). Biến môi trường lấy từ secret
  (`valueFrom`) được gắn vào pod qua webhook.
- Image `btl/batch:dev` đã nạp vào minikube (`minikube image load btl/batch:dev`).

## Lệnh hay dùng

```bash
airflow dags list | grep market_lakehouse
airflow dags trigger market_lakehouse_batch --logical-date 2026-11-25T01:00:00+00:00
airflow dags backfill market_lakehouse_batch -s 2026-11-09 -e 2026-11-25   # chạy bù
kubectl -n spark get sparkapplications
kubectl -n spark logs -l spark-role=driver --tail=100
```

Báo cáo chất lượng của từng ngày nằm ở `s3a://lake/gold/_quality/dt=<ngày>/`.

## Test không cần Airflow

```bash
python -m pytest batch/tests/test_dag.py -q
```

Test kiểm tra thứ tự task, macro Jinja và các trường theo hợp đồng hạ tầng (namespace, SA,
image, secret). Các template đã render cũng qua `kubeconform -strict` với CRD
SparkApplication.
