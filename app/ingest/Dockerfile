FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
ENV SQL_DIR=/app/sql

WORKDIR /app

COPY app/ingest/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app/ingest/taxi_ingest /app/taxi_ingest
COPY sql /app/sql

ENTRYPOINT ["python", "-m", "taxi_ingest.main"]
