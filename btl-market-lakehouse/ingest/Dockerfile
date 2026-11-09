FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

RUN pip install --no-cache-dir \
    confluent-kafka>=2.3 \
    requests>=2.31 \
    websockets>=12.0 \
    yfinance>=0.2.40

COPY common ./common
COPY ingest ./ingest
COPY contracts ./contracts

ENTRYPOINT ["python", "-m", "ingest.run_producer"]
CMD ["--source", "mock", "--dry-run", "--rows", "50"]
