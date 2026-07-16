FROM ghcr.io/astral-sh/uv:0.8.22 AS uv

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /uvx /bin/

WORKDIR /app
COPY apps/mlflow /app/mlflow

RUN uv pip install --system --no-cache /app/mlflow "mlflow>=3.14"

WORKDIR /app/mlflow
EXPOSE 8080

CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
