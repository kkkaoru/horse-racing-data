# Cloudflare Container image for daily finish-position prediction of upcoming
# races. Runs real Python (DuckDB + CatBoost + XGBoost) which a plain Worker
# cannot. Started as a batch job by the finish-position-cron Worker's
# scheduled() handler via container.start({ entrypoint, envVars }).
#
# Build context note: the feature-pipeline scripts live in apps/pc-keiba-viewer.
# DEPLOY.md documents the build so the image is built from the repo root with
# image_build_context set to the repo root, COPYing the viewer pipeline scripts
# in. The wrangler.jsonc in apps/finish-position-cron references this Dockerfile.

FROM python:3.14-slim AS base

# uv for fast, reproducible dependency install (bun/uv only; no pip-by-hand).
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_SYSTEM_PYTHON=1 \
    MODELS_DIR=/models

WORKDIR /app

# System libs the native runtimes need: libpq5 (DuckDB / psycopg2) and libgomp1
# (the GNU OpenMP runtime LightGBM's wheel dynamically links via libgomp.so.1 —
# CatBoost / XGBoost bundle their own OpenMP, but LightGBM does not, so without
# libgomp1 the per-class LightGBM member fails to import at score time).
RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 libgomp1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps from this app's pyproject (locked at build time).
COPY apps/finish-position-predict-container/pyproject.toml ./pyproject.toml
RUN uv pip install --system \
    "catboost>=1.2.10" \
    "duckdb>=1.1.0" \
    "lightgbm>=4.5.0" \
    "pandas>=2.2.0" \
    "psycopg[binary]>=3.2.0" \
    "pyarrow>=17.0.0" \
    "xgboost>=3.2.0"

# The container's own predict entrypoint + pure helpers.
COPY apps/finish-position-predict-container/src /app/src

# Reused, unchanged feature-pipeline scripts from the viewer. predict_upcoming
# imports pipeline_runner (a thin bundled adapter, see DEPLOY.md) which shells
# out to these to build the v8 feature parquet for the upcoming window.
COPY apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py /app/pipeline/
COPY apps/pc-keiba-viewer/src/scripts/finish-position-features /app/pipeline/finish-position-features

# Static lookup parquet baked into the image at /app/lookups (path mirrors
# predict_lib.pipeline_args.COURSE_LOOKUP_PATH). The v8 iter14 course-numerical
# layer joins this 119-row constants table at runtime — tracked in git under
# apps/pc-keiba-viewer/finish-position/lookups/ (see .gitignore negation).
COPY apps/pc-keiba-viewer/finish-position/lookups /app/lookups

# Models are BAKED into the image under MODELS_DIR (/models) so the container has
# no runtime R2-scope dependency. The directory layout mirrors the R2 object keys
# (finish-position/{category}/{modelVersion}/{model.json,metadata.json}) so
# predict_lib.model_meta.build_r2_object_key resolves the baked path unchanged.
# These model artifacts are gitignored scratch (apps/finish-position-predict-
# container/models/, see .gitignore) staged into the build context before deploy.
#
# Per-class artifacts (Phase B-2 ENSEMBLE_ACTIVATION_703) are baked under
#   /models/finish-position/jra/per-class/{kyoso_joken_code}/{model_version}/
# matching predict_lib.model_meta.build_r2_object_key. For class 703 the recursive
# COPY below pulls in the 4 ensemble member single-shot boosters
#   iter20-jra-cb-perclass-703-v8/model.json
#   iter20-jra-cb-perclass-703-hpo-v8/model.json
#   iter21-jra-cb-chain-703-v8/model.json
#   iter22-jra-cb-residual-703-v8/model.json
# plus the iter23-jra-cb-ensemble-703-v8/manifest.json that ensemble_scorer.py
# loads to drive the weighted blend. See docs/finish-position-accuracy/runbook/
# PER_CLASS_ROUTING.md.
COPY apps/finish-position-predict-container/models /models

ENV PYTHONPATH=/app/src:/app/pipeline

# HTTP server mode is the default so CF Containers can probe port 8080 and the
# queue-consumer held-fetch pattern works (container serves /predict + /ping).
# Mac launchd overrides this to empty ("") via docker run -e PREDICT_SERVE_MODE=
# to keep the one-shot CLI batch behaviour unchanged.
ENV PREDICT_SERVE_MODE=http

# Default entrypoint is the daily predictor. The Worker overrides entrypoint /
# envVars per start() call when needed.
ENTRYPOINT ["python", "/app/src/predict_upcoming.py"]
