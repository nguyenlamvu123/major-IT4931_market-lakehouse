PYTHON ?= python3
KUBECONFORM_CRDS := https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json

.PHONY: help venv install samples demo test lint verify smoke \
	dev-up dev-down topics cassandra-init minio-init api branches \
	up down images images-local secrets status port-forward apply apps k8s-topics k8s-validate

help:
	@echo "Laptop (no cluster):"
	@echo "  install         Install Python dependencies"
	@echo "  samples         Generate 20 samples per topic in contracts/samples"
	@echo "  demo / verify   Run the local pandas pipeline and check its output"
	@echo "  test / lint     pytest / compileall + ruff"
	@echo "  api             Run market API on :8000"
	@echo "Docker Compose (Kafka + MinIO + Cassandra, ~6 GB RAM):"
	@echo "  dev-up/dev-down Start/stop the dev stack (buckets + CQL schema created on up)"
	@echo "  topics          Create Kafka topics from contracts/topics.yaml"
	@echo "  cassandra-init  Re-apply contracts/cassandra.cql"
	@echo "  minio-init      Re-create buckets lake, checkpoints"
	@echo "  images-local    Build btl/* images without loading them into minikube"
	@echo "Kubernetes (minikube, 16 GB RAM / 6 CPU):"
	@echo "  up              Build the whole system on a clean machine"
	@echo "  down            Delete it (SOFT=1 keeps minikube)"
	@echo "  images          Build btl/* images for linux/arm64 and minikube image load"
	@echo "  secrets         Create minio-credentials / market-api-keys from .env"
	@echo "  apply           Re-apply platform manifests (kustomize)"
	@echo "  apps            Re-apply role workloads (ingest/k8s, streaming/k8s)"
	@echo "  status          Pods and custom resources per namespace"
	@echo "  port-forward    Grafana :3000, API :8000, Airflow :8080, Prometheus :9090, MinIO :9001"
	@echo "  k8s-topics      Regenerate KafkaTopic CRs from contracts/topics.yaml"
	@echo "  k8s-validate    kustomize build + kubeconform (if installed)"

venv:
	$(PYTHON) -m venv .venv

install:
	. .venv/bin/activate && pip install -r requirements.txt

samples:
	$(PYTHON) scripts/generate_samples.py

demo:
	$(PYTHON) scripts/run_local_demo.py

test:
	$(PYTHON) -m pytest

lint:
	@for d in common ingest streaming batch serving infra scripts tests; do \
		if [ -d $$d ]; then $(PYTHON) -m compileall -q $$d; fi; \
	done
	-$(PYTHON) -m ruff check .

verify:
	$(PYTHON) scripts/verify_pipeline.py

smoke:
	bash scripts/smoke_test.sh

dev-up:
	docker compose -f infra/docker-compose.dev.yml up -d

dev-down:
	docker compose -f infra/docker-compose.dev.yml down -v

topics:
	PYTHON=$(PYTHON) bash infra/scripts/init-topics.sh

cassandra-init:
	bash infra/scripts/init-cassandra.sh

minio-init:
	bash infra/scripts/init-minio.sh

api:
	$(PYTHON) -m uvicorn serving.market_api:app --reload --port 8000

branches:
	git branch -a

up:
	bash infra/scripts/up.sh

down:
	bash infra/scripts/down.sh

images:
	bash infra/scripts/images.sh

images-local:
	MINIKUBE_LOAD=0 PLATFORM= bash infra/scripts/images.sh

secrets:
	bash infra/scripts/secrets.sh

status:
	bash infra/scripts/status.sh

port-forward:
	bash infra/scripts/port-forward.sh

apply:
	bash -c 'source infra/scripts/lib.sh && render_platform | kubectl apply -f -'

apps:
	bash -c 'source infra/scripts/lib.sh && for d in "$${ROLE_K8S_DIRS[@]}"; do apply_role_dir "$$d"; done'

k8s-topics:
	$(PYTHON) -m infra.scripts.gen_kafka_topics

k8s-validate:
	$(PYTHON) -m infra.scripts.gen_kafka_topics --check
	kustomize build --load-restrictor LoadRestrictionsNone infra/k8s > /tmp/btl-rendered.yaml
	kubeconform -strict -summary -ignore-missing-schemas -schema-location default \
		-schema-location '$(KUBECONFORM_CRDS)' /tmp/btl-rendered.yaml infra/k8s/spark/sparkapplication-template.yaml
