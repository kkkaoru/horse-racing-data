# Cloudflare Container image for daily finish-position prediction of upcoming
# races. Runs real Python (DuckDB + CatBoost + XGBoost) which a plain Worker
# cannot. Started as a batch job by the finish-position-cron Worker's
# scheduled() handler via container.start({ entrypoint, envVars }).
#
# Build context note: the feature-pipeline scripts live in apps/pc-keiba-viewer.
# DEPLOY.md documents the build so the image is built from the repo root with
# image_build_context set to the repo root, COPYing the viewer pipeline scripts
# in. The wrangler.jsonc in apps/finish-position-cron references this Dockerfile.

FROM python:3.12-slim AS runtime-base

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

# Build Python's runtime site-packages and the DuckDB extension cache in an
# intermediate stage. Neither uv/uvx nor their download cache is copied into
# the production image.
FROM runtime-base AS python-deps

# uv for fast, reproducible dependency install (bun/uv only; no pip-by-hand).
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install Python deps from this app's pyproject (locked at build time).
COPY apps/finish-position-predict-container/pyproject.toml ./pyproject.toml
RUN uv pip install --system --no-cache \
    "catboost>=1.2.10" \
    "duckdb==1.5.5" \
    "lightgbm>=4.5.0" \
    "numpy>=2.0.0" \
    "pandas>=2.2.0" \
    "psycopg[binary]>=3.2.0" \
    "pyarrow>=17.0.0" \
    "xgboost-cpu==3.2.0"

# Pre-install the DuckDB source extensions into the image so every per-race
# layer subprocess's runtime extension load hits a warm local cache. Production
# prediction reads the Cloudflare Iceberg REST catalog; postgres remains only
# for offline training/parity runs.
#
# The source attach adapter lives in the copied finish-position-features tree.
# It loads iceberg + httpfs for r2-catalog:// sources and postgres otherwise.
# See
# apps/pc-keiba-viewer/src/scripts/finish-position-features/add-similar-race-features.py
# and sibling layer scripts.
RUN python3 -c "import duckdb; connection=duckdb.connect(); connection.execute('INSTALL iceberg'); connection.execute('INSTALL httpfs'); connection.execute('INSTALL postgres')"

# Verify the full local source tree, then copy only the artifacts reachable by
# current production selectors into a fresh tree. The 457 MiB historical model
# directory remains in this intermediate stage and never enters the runtime
# image; the selected finish-position closure is currently about 35 MiB.
FROM runtime-base AS model-artifacts

COPY apps/finish-position-predict-container/src /app/src
COPY apps/finish-position-predict-container/production-artifacts.json /app/production-artifacts.json
COPY apps/finish-position-predict-container/models /artifact-source

ENV PYTHONPATH=/app/src

RUN python -m predict_lib.artifact_integrity \
        --artifact-root /artifact-source \
        --system finish-position \
        --stage-root /selected-models \
    && python -m predict_lib.artifact_integrity \
        --artifact-root /selected-models \
        --system finish-position

FROM runtime-base AS runtime

# Copy only installed runtime packages and pre-installed DuckDB extensions.
# The uv executables and dependency download/cache layers remain in
# python-deps and therefore do not contribute to the production image.
COPY --from=python-deps /usr/local /usr/local
COPY --from=python-deps /root/.duckdb /root/.duckdb

# The container's own predict entrypoint + pure helpers.
COPY apps/finish-position-predict-container/src /app/src

# Tracked digest contract for the models this image is about to bake in below
# (predict_lib.artifact_integrity.MANIFEST_PATH resolves to
# parents[2] of artifact_integrity.py, i.e. /app here).
COPY apps/finish-position-predict-container/production-artifacts.json /app/production-artifacts.json

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
# Per-class routing is currently disabled, so historical per-class artifacts
# are intentionally excluded. If a selector enables them in the future,
# derive_selected_artifact_keys must first include their complete member and
# manifest closure; the staging verifier will then fail closed until every
# selected byte is present in production-artifacts.json.
COPY --from=model-artifacts /selected-models /models

ENV PYTHONPATH=/app/src:/app/pipeline:/app/pipeline/finish-position-features

# Fail-closed artifact-integrity preflight: verify every artifact production
# selectors (model_meta.json / cell_routing.json / the tracked NAR-transformer
# deploy flag) can reach is present under /models with the SHA-256 + size the
# tracked manifest above declares, BEFORE this layer -- and therefore this
# image -- can finish building. A nonzero exit here fails this RUN step, which
# fails `docker build`, which fails `wrangler deploy --containers-rollout ...`
# too, so a corrupt/mismatched/missing selected artifact can never reach
# production through the container build path, even for a caller who runs
# `wrangler deploy` (or `docker build`) directly instead of the cron package's
# `bun run deploy` (which also runs this same verifier earlier, as a fast local
# fail before this slower image build even starts). Running-style artifacts
# are intentionally out of scope here (--system finish-position): they are not
# baked into this container, and their live R2 bytes are attestation-recorded,
# not re-hashed over the network -- see DEPLOY.md's Scope note. The verifier's
# selector-closure check (are all reachable keys manifested at all) still
# always covers running-style regardless of --system, only the BYTE check is
# scoped.
RUN python -m predict_lib.artifact_integrity --artifact-root /models --system finish-position

# HTTP server mode is the default so CF Containers can probe port 8080 and the
# queue-consumer held-fetch pattern works (container serves /predict + /ping).
# Mac launchd overrides this to empty ("") via docker run -e PREDICT_SERVE_MODE=
# to keep the one-shot CLI batch behaviour unchanged.
ENV PREDICT_SERVE_MODE=http

# Default entrypoint is the daily predictor. The Worker overrides entrypoint /
# envVars per start() call when needed.
ENTRYPOINT ["python", "/app/src/predict_upcoming.py"]
