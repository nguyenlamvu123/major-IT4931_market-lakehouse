# btl/airflow:dev - Airflow image for the KubernetesExecutor release (helm/airflow-values.yaml)
# and the docker compose full stack.
#
# Build from btl-market-lakehouse/ on a checkout that contains role C's batch/ directory
# (an integration branch or `make images`, which assembles one):
#   docker buildx build --platform linux/arm64 -f infra/airflow/Dockerfile -t btl/airflow:dev --load .
#
# The official image already ships apache-airflow-providers-cncf-kubernetes, which provides
# SparkKubernetesOperator; nothing has to be pip-installed (and pip must not run as root in
# this image).
FROM apache/airflow:2.9.3-python3.11

ENV MARKET_PROJECT_DIR=/opt/app \
    PYTHONPATH=/opt/app

# DAG files go to the scheduler's DAG folder; the whole batch package (SparkApplication
# templates under batch/k8s/, job code) plus common/ and contracts/ go to /opt/app so DAGs
# can resolve templates through MARKET_PROJECT_DIR.
COPY --chown=airflow:root batch/dags/ /opt/airflow/dags/
COPY --chown=airflow:root batch/ /opt/app/batch/
COPY --chown=airflow:root common/ /opt/app/common/
COPY --chown=airflow:root contracts/ /opt/app/contracts/

USER airflow
