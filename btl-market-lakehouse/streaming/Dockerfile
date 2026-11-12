# Speed layer image: btl/streaming:dev (role B).
#
# Build from the project root (the image needs common/, contracts/ and streaming/):
#   docker buildx build --platform linux/arm64 -f streaming/Dockerfile -t btl/streaming:dev --load .
#   minikube image load btl/streaming:dev
#
# The connector jars are downloaded at build time instead of `--packages` at submit
# time: the driver and executors start without reaching Maven Central, and every
# restart (restartPolicy Always) is fast and reproducible.
ARG SPARK_IMAGE=apache/spark:3.5.3-scala2.12-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

ARG SPARK_VERSION=3.5.3
ARG SCALA=2.12
ARG KAFKA_CLIENTS=3.4.1
ARG COMMONS_POOL2=2.11.1
ARG CASSANDRA_CONNECTOR=3.5.1
ARG HADOOP_AWS=3.3.4
ARG AWS_SDK_BUNDLE=1.12.262
ARG MAVEN=https://repo1.maven.org/maven2

USER root
RUN set -eux; \
    cd /opt/spark/jars; \
    for jar in \
      "org/apache/spark/spark-sql-kafka-0-10_${SCALA}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA}-${SPARK_VERSION}.jar" \
      "org/apache/spark/spark-token-provider-kafka-0-10_${SCALA}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA}-${SPARK_VERSION}.jar" \
      "org/apache/kafka/kafka-clients/${KAFKA_CLIENTS}/kafka-clients-${KAFKA_CLIENTS}.jar" \
      "org/apache/commons/commons-pool2/${COMMONS_POOL2}/commons-pool2-${COMMONS_POOL2}.jar" \
      "com/datastax/spark/spark-cassandra-connector-assembly_${SCALA}/${CASSANDRA_CONNECTOR}/spark-cassandra-connector-assembly_${SCALA}-${CASSANDRA_CONNECTOR}.jar" \
      "org/apache/hadoop/hadoop-aws/${HADOOP_AWS}/hadoop-aws-${HADOOP_AWS}.jar" \
      "com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE}.jar"; \
    do curl -fsSLO "${MAVEN}/${jar}"; done; \
    chmod 644 /opt/spark/jars/*.jar

# Driver-side Python: pandas/pyarrow for toPandas() + indicator state, prometheus-client
# for the :9108 metrics endpoint. setuptools provides `distutils`, which PySpark 3.5
# still imports in toPandas() and which Python >= 3.12 no longer ships.
RUN pip install --no-cache-dir \
      "pandas>=2.2,<3" "numpy>=1.26" "pyarrow>=15" "prometheus-client>=0.20" "setuptools>=70"

WORKDIR /opt/app
ENV PYTHONPATH=/opt/app PYTHONUNBUFFERED=1
COPY common /opt/app/common
COPY contracts /opt/app/contracts
COPY streaming /opt/app/streaming

EXPOSE 9108
USER 185

# Spark Operator overrides the entrypoint and submits
# local:///opt/app/streaming/job.py; this default only serves `docker run` smoke tests.
CMD ["/opt/spark/bin/spark-submit", "--master", "local[2]", "/opt/app/streaming/job.py", "--source", "files", "--input", "/opt/app/contracts/samples", "--available-now", "--output-mode", "parquet", "--parquet-path", "/tmp/speed", "--checkpoint-root", "/tmp/checkpoints", "--metrics-port", "0"]
