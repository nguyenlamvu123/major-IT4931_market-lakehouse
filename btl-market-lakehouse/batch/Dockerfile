# Image btl/batch:dev: PySpark batch jobs of role C, run by the Spark Operator.
#
# Build from the project root (the jobs import common/ and read contracts/cassandra.cql):
#   docker buildx build --platform linux/arm64 -f batch/Dockerfile -t btl/batch:dev .   # Mac M1 cluster
#   docker build -f batch/Dockerfile -t btl/batch:dev .                                 # same-arch host
#   minikube image load btl/batch:dev
#
# The base image is multi-arch (amd64/arm64), Ubuntu 22.04 with Python 3.10 and Java 17.
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# Jars that spark-submit --packages would otherwise download at every job start:
# - hadoop-aws + aws-java-sdk-bundle: s3a:// access to MinIO (versions match the Hadoop
#   3.3.4 client bundled with Spark 3.5.3)
# - spark-cassandra-connector assembly: gold -> Cassandra (cassandra_sink, train_kmeans)
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG CASSANDRA_CONNECTOR_VERSION=3.5.1
RUN set -eux; \
    cd /opt/spark/jars; \
    for url in \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/${CASSANDRA_CONNECTOR_VERSION}/spark-cassandra-connector-assembly_2.12-${CASSANDRA_CONNECTOR_VERSION}.jar"; \
    do wget -q "$url"; done; \
    chmod 644 /opt/spark/jars/*.jar

# common.analytics (reference maths reused by gold) needs pandas/numpy; PyYAML for tools.
RUN pip install --no-cache-dir "pandas>=2.2,<3" "numpy>=1.26,<3" "pyarrow>=15" "PyYAML>=6.0"

WORKDIR /opt/app
COPY common /opt/app/common
COPY contracts /opt/app/contracts
COPY batch /opt/app/batch
ENV PYTHONPATH=/opt/app

# Non-root user of the apache/spark image.
USER spark
