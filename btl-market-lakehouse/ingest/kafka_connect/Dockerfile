# Kafka Connect image with the Confluent S3 sink plugin (btl/kafka-connect-s3:dev).
#
# Default target: Strimzi KafkaConnect on Kubernetes. STRIMZI_VERSION/KAFKA_VERSION must
# match the Strimzi operator installed by `make up` (role D) and spec.version of
# ingest/k8s/kafka-connect/kafka-connect.yaml.
#
#   docker buildx build --platform linux/arm64 -f ingest/kafka_connect/Dockerfile \
#     -t btl/kafka-connect-s3:dev ingest/kafka_connect && minikube image load btl/kafka-connect-s3:dev
#
# Docker Compose variant (plain Connect worker, no operator):
#
#   docker build -f ingest/kafka_connect/Dockerfile \
#     --build-arg BASE_IMAGE=confluentinc/cp-kafka-connect:7.7.1 \
#     --build-arg PLUGIN_PATH=/usr/share/confluent-hub-components \
#     --build-arg RUNTIME_USER=appuser -t btl/kafka-connect-s3:compose ingest/kafka_connect
ARG STRIMZI_VERSION=0.45.0
ARG KAFKA_VERSION=3.9.0
ARG BASE_IMAGE=quay.io/strimzi/kafka:${STRIMZI_VERSION}-kafka-${KAFKA_VERSION}
ARG S3_CONNECTOR_VERSION=10.5.17

FROM alpine:3.20 AS plugin
ARG S3_CONNECTOR_VERSION
# Confluent Hub serves each connector as a zip with lib/ inside; Connect scans the
# unpacked directory through plugin.path.
RUN apk add --no-cache curl unzip \
 && curl -fsSL -o /tmp/s3.zip \
    "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/${S3_CONNECTOR_VERSION}/confluentinc-kafka-connect-s3-${S3_CONNECTOR_VERSION}.zip" \
 && mkdir -p /plugins \
 && unzip -q /tmp/s3.zip -d /plugins

FROM ${BASE_IMAGE}
# Strimzi images load plugins from /opt/kafka/plugins.
ARG PLUGIN_PATH=/opt/kafka/plugins
ARG RUNTIME_USER=1001
USER root
COPY --from=plugin /plugins/ ${PLUGIN_PATH}/
USER ${RUNTIME_USER}
