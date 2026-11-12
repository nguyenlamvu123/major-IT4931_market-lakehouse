#!/usr/bin/env bash
# make up: build the whole system on a clean machine (PDF, việc D.3).
#
# Order matters:
#   1. minikube                      (16 GB / 6 CPU, PDF "Máy móc")
#   2. namespaces + Secrets          (Helm releases and pods read them at start)
#   3. cert-manager                  (k8ssandra-operator webhooks need certificates)
#   4. kube-prometheus-stack         (installs ServiceMonitor/PodMonitor CRDs used below)
#   5. Strimzi, k8ssandra-operator, spark-operator
#   6. platform CRs/manifests        (kustomize: Kafka, topics, MinIO, K8ssandra, jobs, API)
#   7. wait for Kafka, Cassandra, bucket and schema jobs
#   8. Airflow                       (needs its postgres from step 6)
#   9. images + role workloads       (A producers/Connect, B speed layer) if present
#
# Every step is idempotent (helm upgrade --install, kubectl apply), so re-running after a
# failure continues where it stopped. Tunables: MINIKUBE_CPUS, MINIKUBE_MEMORY,
# MINIKUBE_DISK, SKIP_IMAGES=1, SKIP_APPS=1.
set -euo pipefail
# shellcheck source=infra/scripts/lib.sh
source "$(dirname "$0")/lib.sh"
# shellcheck source=infra/k8s/helm/versions.env
source "${HELM_DIR}/versions.env"
need minikube kubectl helm

MINIKUBE_CPUS="${MINIKUBE_CPUS:-6}"
MINIKUBE_MEMORY="${MINIKUBE_MEMORY:-16g}"
MINIKUBE_DISK="${MINIKUBE_DISK:-40g}"

log "1/9 minikube (${MINIKUBE_CPUS} CPU, ${MINIKUBE_MEMORY})"
if ! minikube -p "${MINIKUBE_PROFILE}" status >/dev/null 2>&1; then
  minikube start -p "${MINIKUBE_PROFILE}" \
    --cpus "${MINIKUBE_CPUS}" --memory "${MINIKUBE_MEMORY}" --disk-size "${MINIKUBE_DISK}"
fi
# NOTICE: on the M1 host the operators lost their leader lease when macOS slept (idea PDF,
# slide "Sự cố về hệ thống"). Keep the machine awake while the cluster runs
# (`caffeinate -dims` on macOS); the operators recover by themselves afterwards.

log "2/9 namespaces + secrets"
kubectl apply -f "${K8S}/namespaces.yaml"
bash "${HERE}/secrets.sh"

log "3/9 helm repositories"
helm repo add jetstack https://charts.jetstack.io --force-update >/dev/null
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update >/dev/null
helm repo add strimzi https://strimzi.io/charts/ --force-update >/dev/null
helm repo add k8ssandra https://helm.k8ssandra.io/stable --force-update >/dev/null
helm repo add spark-operator https://kubeflow.github.io/spark-operator --force-update >/dev/null
helm repo add apache-airflow https://airflow.apache.org --force-update >/dev/null
helm repo update >/dev/null

release() {
  # release <name> <chart> <namespace> <version> <values-file> [extra helm args...]
  local name="$1" chart="$2" namespace="$3" version="$4" values="$5"
  shift 5
  log "helm ${name} (${chart} ${version}) -> ns ${namespace}"
  helm upgrade --install "${name}" "${chart}" --namespace "${namespace}" --create-namespace \
    --version "${version}" -f "${HELM_DIR}/${values}" --wait --timeout 15m "$@"
}

log "4/9 operators"
release cert-manager jetstack/cert-manager cert-manager "${CERT_MANAGER_CHART_VERSION}" cert-manager-values.yaml
release kube-prometheus-stack prometheus-community/kube-prometheus-stack monitoring \
  "${KUBE_PROMETHEUS_STACK_CHART_VERSION}" kube-prometheus-stack-values.yaml
release strimzi strimzi/strimzi-kafka-operator kafka "${STRIMZI_CHART_VERSION}" strimzi-values.yaml
release k8ssandra-operator k8ssandra/k8ssandra-operator cassandra "${K8SSANDRA_CHART_VERSION}" \
  k8ssandra-operator-values.yaml
release spark-operator spark-operator/spark-operator spark "${SPARK_OPERATOR_CHART_VERSION}" \
  spark-operator-values.yaml

log "5/9 platform manifests (Kafka, topics, MinIO, K8ssandra, jobs, market-api, monitors)"
render_platform | kubectl apply -f -

log "6/9 wait for Kafka, MinIO buckets, Cassandra schema"
kubectl -n kafka wait kafka/market --for=condition=Ready --timeout=900s
kubectl -n minio wait job/minio-buckets --for=condition=complete --timeout=600s
# cass-operator labels Cassandra pods with the cluster name; the schema Job itself waits
# for CQL, so this only bounds how long we block.
kubectl -n cassandra wait pod -l cassandra.datastax.com/cluster=market \
  --for=condition=Ready --timeout=1200s
kubectl -n cassandra wait job/cassandra-schema --for=condition=complete --timeout=900s

log "7/9 Airflow (KubernetesExecutor)"
kubectl -n airflow rollout status deploy/airflow-postgres --timeout=300s
release airflow apache-airflow/airflow airflow "${AIRFLOW_CHART_VERSION}" airflow-values.yaml

if [[ "${SKIP_IMAGES:-0}" != "1" ]]; then
  log "8/9 images (linux/arm64 by default) -> minikube"
  if command -v docker >/dev/null 2>&1; then
    bash "${HERE}/images.sh"
  else
    warn "docker not found; skipping image build (pods using btl/* images stay Pending)"
  fi
fi

if [[ "${SKIP_APPS:-0}" != "1" ]]; then
  log "9/9 role workloads"
  for dir in "${ROLE_K8S_DIRS[@]}"; do
    apply_role_dir "${dir}"
  done
fi

log "done"
bash "${HERE}/status.sh"
cat <<'MSG'

Next:
  make port-forward   # Grafana :3000, market-api :8000, Airflow :8080, Prometheus :9090, MinIO :9001
  Grafana login admin/admin -> folder "Market Lakehouse"
MSG
