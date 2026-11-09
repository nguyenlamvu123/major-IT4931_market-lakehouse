PYTHON ?= python3

.PHONY: help venv install samples demo test lint verify smoke dev-up dev-down topics cassandra-init api branches

help:
	@echo "Targets:"
	@echo "  install         Install Python dependencies"
	@echo "  samples         Generate JSONL samples in contracts/samples"
	@echo "  demo            Run local pandas pipeline (no Kafka/Spark needed)"
	@echo "  test            Run pytest"
	@echo "  lint            Run compileall + ruff (if installed)"
	@echo "  verify          Verify demo output"
	@echo "  smoke           Smoke-test API/Prometheus/Grafana"
	@echo "  dev-up/down     Start/stop Kafka+MinIO+Cassandra dev stack"
	@echo "  topics          Create Kafka topics"
	@echo "  cassandra-init  Create keyspace/table"
	@echo "  api             Run market API on :8000"
	@echo "  branches        List git branches"

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
	@for d in common ingest streaming batch serving scripts tests; do \
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
	bash infra/scripts/init-topics.sh

cassandra-init:
	bash infra/scripts/init-cassandra.sh

api:
	$(PYTHON) -m uvicorn serving.market_api:app --reload --port 8000

branches:
	git branch -a
