# Cloudflare Container image for daily finish-position prediction of upcoming
# races. Runs real Python (DuckDB + CatBoost + XGBoost) which a plain Worker
# cannot. Started as a batch job by the finish-position-cron Worker's
# scheduled() handler via container.start({ entrypoint, envVars }).
#
# Build context note: the feature-pipeline scripts live in apps/pc-keiba-viewer.
# DEPLOY.md documents the build so the image is built from the repo root with
# image_build_context set to the repo root, COPYing the viewer pipeline scripts
# in. The wrangler.jsonc in apps/finish-position-cron references this Dockerfile.

FROM python:3.12-slim AS base

# uv for fast, reproducible dependency install (bun/uv only; no pip-by-hand).
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_SYSTEM_PYTHON=1 \
    MODELS_DIR=/models

WORKDIR /app

# System libs DuckDB / psycopg2 need at runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps from this app's pyproject (locked at build time).
COPY apps/finish-position-predict-container/pyproject.toml ./pyproject.toml
RUN uv pip install --system \
    "catboost>=1.2.10" \
    "duckdb>=1.1.0" \
    "pandas>=2.2.0" \
    "psycopg[binary]>=3.2.0" \
    "pyarrow>=17.0.0" \
    "xgboost>=3.2.0"

# The container's own predict entrypoint + pure helpers.
COPY apps/finish-position-predict-container/src /app/src

# Reused, unchanged feature-pipeline scripts from the viewer. predict_upcoming
# imports pipeline_runner (a thin bundled adapter, see DEPLOY.md) which shells
# out to these to build the v7-lineage feature parquet for the upcoming window.
COPY apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py /app/pipeline/
COPY apps/pc-keiba-viewer/src/scripts/finish-position-features /app/pipeline/finish-position-features

# Models are NOT baked in — they are pulled from R2
# (finish-position/{category}/{modelVersion}/) into MODELS_DIR by the deploy /
# warm step. See DEPLOY.md. The image stays small and model updates need no
# rebuild.

ENV PYTHONPATH=/app/src:/app/pipeline

# Default entrypoint is the daily predictor. The Worker overrides entrypoint /
# envVars per start() call when needed.
ENTRYPOINT ["python", "/app/src/predict_upcoming.py"]
