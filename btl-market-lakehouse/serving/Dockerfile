FROM python:3.11-slim

WORKDIR /app
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

COPY serving/requirements.txt ./serving/requirements.txt
RUN pip install --no-cache-dir -r serving/requirements.txt

COPY common ./common
COPY serving ./serving

EXPOSE 8000
CMD ["uvicorn", "serving.market_api:app", "--host", "0.0.0.0", "--port", "8000"]
