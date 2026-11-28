FROM ghcr.io/astral-sh/uv:0.12.13 AS uv

FROM python:3.15-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /uvx /bin/

WORKDIR /app
COPY apps/mlflow /app/mlflow

# Exact pin in lockstep with apps/mlflow + apps/mlflow-ui: an unpinned build pulled a newer
# mlflow whose server migrated the shared Neon schema and broke every older client.
RUN uv pip install --system --no-cache /app/mlflow "mlflow==3.17.1"

WORKDIR /app/mlflow
EXPOSE 8080

CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
