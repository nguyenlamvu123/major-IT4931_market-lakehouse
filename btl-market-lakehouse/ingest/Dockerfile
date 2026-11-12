# btl/ingest:dev - every producer of role A in one image; choose one with --source.
#
# Build from the project root (the image also needs common/ and contracts/):
#   docker buildx build --platform linux/arm64 -f ingest/Dockerfile -t btl/ingest:dev .
#   minikube image load btl/ingest:dev
# linux/arm64 is the MacBook M1 that hosts minikube (PDF section 6, "Máy móc"); drop
# --platform when building on the machine that runs the cluster.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    METRICS_PORT=8001 \
    XDG_CACHE_HOME=/tmp/cache

WORKDIR /app

COPY ingest/requirements.txt ./ingest/requirements.txt
RUN pip install --no-cache-dir -r ingest/requirements.txt

COPY common ./common
COPY contracts ./contracts
COPY ingest ./ingest

# Non-root; Kubernetes additionally mounts /tmp as the only writable path (yfinance
# keeps its timezone cache under XDG_CACHE_HOME).
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin producer
USER 10001

EXPOSE 8001
ENTRYPOINT ["python", "-m", "ingest.run_producer"]
CMD ["--source", "mock", "--dry-run", "--rows", "50"]
