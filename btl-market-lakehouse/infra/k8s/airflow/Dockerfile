FROM apache/airflow:2.9.2

USER root
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark
WORKDIR /opt/app

COPY batch /opt/app/batch
COPY common /opt/app/common
COPY contracts /opt/app/contracts
COPY batch/dags /opt/airflow/dags

USER airflow
