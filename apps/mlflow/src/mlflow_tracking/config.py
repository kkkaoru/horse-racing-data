"""Environment / path configuration for the horse-racing MLflow tracking helper.

Centralizes every path and environment-variable lookup so the CLI, the
backfill scripts, and the tests share one source of truth for where the
tracking database, artifact store, and R2 credentials live.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Final, Literal, Protocol

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# __file__ = apps/mlflow/src/mlflow_tracking/config.py
#   .parents[0] -> apps/mlflow/src/mlflow_tracking
#   .parents[1] -> apps/mlflow/src
#   .parents[2] -> apps/mlflow
#   .parents[3] -> apps
#   .parents[4] -> repo root
REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[4]

ENV_DATA_DIR: Final[str] = "HORSE_RACING_MLFLOW_DATA_DIR"
ENV_BACKEND_URI: Final[str] = "HORSE_RACING_MLFLOW_BACKEND_URI"
ENV_TRACKING_URI: Final[str] = "MLFLOW_TRACKING_URI"
ENV_ARTIFACTS_MODE: Final[str] = "HORSE_RACING_MLFLOW_ARTIFACTS_MODE"
ENV_R2_BUCKET: Final[str] = "HORSE_RACING_MLFLOW_R2_BUCKET"
ENV_R2_PREFIX: Final[str] = "HORSE_RACING_MLFLOW_R2_PREFIX"
DEFAULT_R2_PREFIX: Final[str] = "mlflow"

# Override the default env-file path each loader reads when its own
# `env_file` argument is omitted. Primarily so test suites can point these at
# a guaranteed-nonexistent path and make the loader a no-op deterministically,
# without depending on whether the real, gitignored files happen to exist on
# the machine running the tests, or on their contents.
ENV_ENV_FILE: Final[str] = "HORSE_RACING_MLFLOW_ENV_FILE"
ENV_ROOT_ENV_FILE: Final[str] = "HORSE_RACING_MLFLOW_ROOT_ENV_FILE"

# Repo-standard R2 env names, adopted from apps/horse-racing-data-lake/.env.example
# and .dev.vars.example: CLOUDFLARE_ACCOUNT_ID, R2_ACCESS_KEY_ID,
# R2_SECRET_ACCESS_KEY. R2_ACCOUNT_ID is accepted first for parity with the
# generic S3-style naming other external tooling expects; CLOUDFLARE_ACCOUNT_ID
# is the fallback that already exists elsewhere in this repo.
ENV_R2_ACCOUNT_ID: Final[str] = "R2_ACCOUNT_ID"
ENV_CLOUDFLARE_ACCOUNT_ID: Final[str] = "CLOUDFLARE_ACCOUNT_ID"
ENV_R2_ACCESS_KEY_ID: Final[str] = "R2_ACCESS_KEY_ID"
ENV_R2_SECRET_ACCESS_KEY: Final[str] = "R2_SECRET_ACCESS_KEY"
ENV_MLFLOW_S3_ENDPOINT_URL: Final[str] = "MLFLOW_S3_ENDPOINT_URL"
ENV_AWS_ACCESS_KEY_ID: Final[str] = "AWS_ACCESS_KEY_ID"
ENV_AWS_SECRET_ACCESS_KEY: Final[str] = "AWS_SECRET_ACCESS_KEY"
ENV_AWS_DEFAULT_REGION: Final[str] = "AWS_DEFAULT_REGION"

ArtifactsMode = Literal["local", "r2"]
_ARTIFACTS_MODES: Final[frozenset[str]] = frozenset({"local", "r2"})

# Canonical experiment names, shared across the backfill/ingest modules and
# the `init` CLI command so they never drift from one another.
EXPERIMENT_FP_REGISTRY_BACKFILL: Final[str] = "finish-position/registry-backfill"
EXPERIMENT_FP_WF_EVAL: Final[str] = "finish-position/wf-eval"
EXPERIMENT_FP_SERVE_ACCURACY: Final[str] = "finish-position/serve-accuracy"
EXPERIMENT_RS_REGISTRY_BACKFILL: Final[str] = "running-style/registry-backfill"
EXPERIMENT_RS_EVAL: Final[str] = "running-style/eval"
# One persistent metric-timeline run per (task, category) pair lives here --
# see timeline.py's module docstring for why this is a separate experiment
# rather than reusing finish-position/serve-accuracy or running-style/eval
# (those hold one-point-per-day runs; this holds one growing-series run).
EXPERIMENT_TIMELINES: Final[str] = "timelines"
ALL_EXPERIMENT_NAMES: Final[tuple[str, ...]] = (
    EXPERIMENT_FP_REGISTRY_BACKFILL,
    EXPERIMENT_FP_WF_EVAL,
    EXPERIMENT_FP_SERVE_ACCURACY,
    EXPERIMENT_RS_REGISTRY_BACKFILL,
    EXPERIMENT_RS_EVAL,
    EXPERIMENT_TIMELINES,
)


def _strip_matching_quotes(value: str) -> str:
    """Strip one layer of matching single or double quotes from `value`."""
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
        return value[1:-1]
    return value


def load_dotenv_local(env_file: Path | None = None) -> None:
    """Load KEY=VALUE pairs from a .env-style file into os.environ.

    Never overrides a variable already present in os.environ -- an explicit
    process env value always wins over the file. This is a soft/best-effort
    loader, never a hard gate: it no-ops when the file does not exist, and
    silently skips blank lines, '#'-comment lines, and malformed lines
    (missing '='). One layer of matching single or double quotes is
    stripped from each value.

    Defaults to apps/mlflow/.env.local -- do not change this default path.
    It is the canonical, gitignored file that mlflow_ui.config's equivalent
    loader also targets, since both packages need to resolve the same
    backend URI / R2 settings. Unlike the repo-root .env (loaded via
    direnv's interactive-shell hook), this file is read directly by the
    process itself, so it also works for non-interactive callers (cron,
    launchd, subprocess) that never source an interactive shell.

    The explicit `env_file` argument always wins. When omitted, the
    HORSE_RACING_MLFLOW_ENV_FILE env var (if set) names the file to load
    instead of the default -- primarily so test suites can point this at a
    guaranteed-nonexistent path and make every call a no-op, rather than
    depending on whether the real .env.local exists on the machine running
    the tests.
    """
    if env_file is not None:
        path = env_file
    else:
        override = os.environ.get(ENV_ENV_FILE, "").strip()
        path = Path(override) if override else REPO_ROOT / "apps" / "mlflow" / ".env.local"
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep:
            continue
        key = key.strip()
        value = _strip_matching_quotes(value.strip())
        os.environ.setdefault(key, value)


_ROOT_ENV_ALLOWED_PREFIXES: Final[tuple[str, ...]] = ("HORSE_RACING_MLFLOW_", "MLFLOW_", "R2_")
_ROOT_ENV_ALLOWED_EXACT: Final[frozenset[str]] = frozenset({"CLOUDFLARE_ACCOUNT_ID"})


def _is_allowed_root_env_key(key: str) -> bool:
    """Return True if `key` may be imported from the repo-root .env fallback."""
    return key in _ROOT_ENV_ALLOWED_EXACT or key.startswith(_ROOT_ENV_ALLOWED_PREFIXES)


def load_repo_root_env_fallback(env_file: Path | None = None) -> None:
    """Fallback layer below load_dotenv_local(): parse specific keys directly
    out of the repo-root .env (NOT via direnv) so mlflow_tracking resolves
    Neon/R2 config even when apps/mlflow/.env.local is missing or
    incomplete. Root .env holds many unrelated repo secrets, so only keys
    matching HORSE_RACING_MLFLOW_*/MLFLOW_*/R2_* or exactly
    CLOUDFLARE_ACCOUNT_ID are ever imported -- everything else in that file
    is ignored. Uses os.environ.setdefault, so any var already set (by the
    process environment OR by an earlier load_dotenv_local() call) always
    wins. Tolerates an optional leading 'export ' on a line. Never raises
    on a missing file or a malformed line.

    The explicit `env_file` argument always wins. When omitted, the
    HORSE_RACING_MLFLOW_ROOT_ENV_FILE env var (if set) names the file to load
    instead of the default -- same rationale as load_dotenv_local()'s
    HORSE_RACING_MLFLOW_ENV_FILE override.
    """
    if env_file is not None:
        path = env_file
    else:
        override = os.environ.get(ENV_ROOT_ENV_FILE, "").strip()
        path = Path(override) if override else REPO_ROOT / ".env"
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip(" ")
        key, sep, value = line.partition("=")
        if not sep:
            continue
        key = key.strip()
        if not _is_allowed_root_env_key(key):
            continue
        value = _strip_matching_quotes(value.strip())
        os.environ.setdefault(key, value)


def default_data_dir(repo_root: Path = REPO_ROOT) -> Path:
    """Return the default on-disk data directory for the tracking store."""
    return repo_root / "apps" / "mlflow" / "data"


def get_data_dir(repo_root: Path = REPO_ROOT) -> Path:
    """Return the data directory, honoring HORSE_RACING_MLFLOW_DATA_DIR."""
    override = os.environ.get(ENV_DATA_DIR)
    if override:
        return Path(override)
    return default_data_dir(repo_root)


def db_path_for(data_dir: Path) -> Path:
    """Return the sqlite database file path under a data directory."""
    return data_dir / "mlflow.db"


def default_tracking_uri(data_dir: Path) -> str:
    """Build the sqlite tracking URI for a data directory.

    ``sqlite:///`` (3 literal slashes) plus a posix absolute path (itself
    starting with ``/``) yields the 4-slash form SQLAlchemy expects for an
    absolute sqlite path, e.g. ``sqlite:////Users/x/mlflow.db``.
    """
    abs_path = db_path_for(data_dir).resolve()
    return f"sqlite:///{abs_path.as_posix()}"


def get_tracking_uri(data_dir: Path | None = None) -> str:
    """Return the tracking URI.

    HORSE_RACING_MLFLOW_BACKEND_URI (repo-scoped, wins) takes precedence over
    the standard MLFLOW_TRACKING_URI (honored for interop with generic mlflow
    tooling/docs), which in turn takes precedence over the computed default.
    """
    backend_override = os.environ.get(ENV_BACKEND_URI)
    if backend_override:
        return backend_override
    generic_override = os.environ.get(ENV_TRACKING_URI)
    if generic_override:
        return generic_override
    resolved_dir = data_dir if data_dir is not None else get_data_dir()
    return default_tracking_uri(resolved_dir)


def default_artifact_root(data_dir: Path) -> str:
    """Build the file:// artifact root URI for a data directory."""
    abs_path = (data_dir / "mlartifacts").resolve()
    return f"file://{abs_path.as_posix()}"


def get_artifacts_mode() -> ArtifactsMode:
    """Return the configured artifact-store mode ("local" default, or "r2")."""
    raw = os.environ.get(ENV_ARTIFACTS_MODE, "local").strip().lower()
    if raw not in _ARTIFACTS_MODES:
        raise ValueError(f"{ENV_ARTIFACTS_MODE} must be 'local' or 'r2', got: {raw!r}")
    return "r2" if raw == "r2" else "local"


def get_r2_bucket() -> str:
    """Return the R2 bucket name for artifact storage; raises when unset."""
    bucket = os.environ.get(ENV_R2_BUCKET, "").strip()
    if not bucket:
        raise ValueError(f"{ENV_R2_BUCKET} must be set when artifacts mode is 'r2'")
    return bucket


def get_r2_prefix() -> str:
    """Return the R2 key prefix, defaulting to 'mlflow'."""
    return os.environ.get(ENV_R2_PREFIX, DEFAULT_R2_PREFIX).strip() or DEFAULT_R2_PREFIX


def build_r2_artifact_location(bucket: str, prefix: str) -> str:
    """Build the s3:// artifact-location URI for an R2 bucket/prefix."""
    return f"s3://{bucket}/{prefix}"


def resolve_artifact_location(data_dir: Path | None = None) -> str:
    """Return the artifact-location URI to use for newly created experiments.

    "local" mode (default) returns the file:// mlartifacts root under the data
    directory; "r2" mode returns the s3:// location built from
    HORSE_RACING_MLFLOW_R2_BUCKET / HORSE_RACING_MLFLOW_R2_PREFIX.
    """
    if get_artifacts_mode() == "r2":
        return build_r2_artifact_location(get_r2_bucket(), get_r2_prefix())
    resolved_dir = data_dir if data_dir is not None else get_data_dir()
    return default_artifact_root(resolved_dir)


def ensure_wal(db_path: Path) -> None:
    """Create the parent directory if needed and switch the sqlite file to WAL mode."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    finally:
        conn.close()


def apply_r2_env() -> None:
    """Map repo-standard R2 envs onto the S3-compatible envs mlflow's boto3 client reads.

    Only fills in a variable when it is unset, so an operator's explicit
    AWS_*/MLFLOW_S3_ENDPOINT_URL override always wins. Safe to call
    unconditionally in "local" mode: it is a no-op when no R2 envs are set.
    """
    account_id = os.environ.get(ENV_R2_ACCOUNT_ID) or os.environ.get(ENV_CLOUDFLARE_ACCOUNT_ID)
    if account_id and ENV_MLFLOW_S3_ENDPOINT_URL not in os.environ:
        os.environ[ENV_MLFLOW_S3_ENDPOINT_URL] = f"https://{account_id}.r2.cloudflarestorage.com"
    access_key = os.environ.get(ENV_R2_ACCESS_KEY_ID)
    if access_key and ENV_AWS_ACCESS_KEY_ID not in os.environ:
        os.environ[ENV_AWS_ACCESS_KEY_ID] = access_key
    secret_key = os.environ.get(ENV_R2_SECRET_ACCESS_KEY)
    if secret_key and ENV_AWS_SECRET_ACCESS_KEY not in os.environ:
        os.environ[ENV_AWS_SECRET_ACCESS_KEY] = secret_key
    if ENV_AWS_DEFAULT_REGION not in os.environ:
        os.environ[ENV_AWS_DEFAULT_REGION] = "auto"


class _HeadBucketClient(Protocol):
    # boto3's real head_bucket only accepts a capitalized `Bucket=` keyword;
    # **kwargs (rather than a `Bucket: str` parameter) avoids re-declaring
    # that non-lowercase name here while still letting the call site below
    # pass it through unchanged.
    def head_bucket(self, **kwargs: str) -> object: ...


def check_r2_bucket_reachable(bucket: str, *, client: _HeadBucketClient | None = None) -> bool:
    """Return True if `bucket` responds to a HeadBucket call; False on any client error.

    Never raises — used as a soft warning during `init`, not a hard gate, so a
    misconfigured or offline R2 endpoint never blocks local tracking-store use.
    """
    s3_client = client if client is not None else boto3.client("s3")
    try:
        s3_client.head_bucket(Bucket=bucket)
    except (ClientError, BotoCoreError):
        return False
    return True
