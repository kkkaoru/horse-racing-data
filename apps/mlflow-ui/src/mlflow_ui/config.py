"""Configuration for the MLflow tracking UI launcher.

All knobs are read from ``HORSE_RACING_MLFLOW_*`` / ``R2_*`` environment
variables (see README.md for the full table). Nothing here touches the
filesystem at import time -- directory/file creation happens lazily in
server.py when the server is actually started.
"""

from __future__ import annotations

import os
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

REPO_ROOT: Path = Path(__file__).resolve().parents[4]

DEFAULT_DATA_DIR: Path = REPO_ROOT / "apps" / "mlflow" / "data"
DEFAULT_ENV_LOCAL_FILE: Path = REPO_ROOT / "apps" / "mlflow" / ".env.local"
DEFAULT_ROOT_ENV_FILE: Path = REPO_ROOT / ".env"
DEFAULT_HOST: str = "127.0.0.1"
DEFAULT_PORT: int = 5252
DEFAULT_R2_PREFIX: str = "mlflow"

ArtifactsMode = Literal["local", "r2"]

_DATA_DIR_ENV: str = "HORSE_RACING_MLFLOW_DATA_DIR"
_BACKEND_URI_ENV: str = "HORSE_RACING_MLFLOW_BACKEND_URI"
_ARTIFACTS_MODE_ENV: str = "HORSE_RACING_MLFLOW_ARTIFACTS_MODE"
_HOST_ENV: str = "HORSE_RACING_MLFLOW_UI_HOST"
_PORT_ENV: str = "HORSE_RACING_MLFLOW_UI_PORT"
_R2_BUCKET_ENV: str = "HORSE_RACING_MLFLOW_R2_BUCKET"
_R2_PREFIX_ENV: str = "HORSE_RACING_MLFLOW_R2_PREFIX"

_ENV_FILE_ENV: str = "HORSE_RACING_MLFLOW_ENV_FILE"
_ROOT_ENV_FILE_ENV: str = "HORSE_RACING_MLFLOW_ROOT_ENV_FILE"

_R2_ACCOUNT_ID_ENV: str = "R2_ACCOUNT_ID"
_CLOUDFLARE_ACCOUNT_ID_ENV: str = "CLOUDFLARE_ACCOUNT_ID"
_R2_ACCESS_KEY_ID_ENV: str = "R2_ACCESS_KEY_ID"
_R2_SECRET_ACCESS_KEY_ENV: str = "R2_SECRET_ACCESS_KEY"

_AWS_ACCESS_KEY_ID_ENV: str = "AWS_ACCESS_KEY_ID"
_AWS_SECRET_ACCESS_KEY_ENV: str = "AWS_SECRET_ACCESS_KEY"
_AWS_DEFAULT_REGION_ENV: str = "AWS_DEFAULT_REGION"
_MLFLOW_S3_ENDPOINT_URL_ENV: str = "MLFLOW_S3_ENDPOINT_URL"

_MLFLOW_SERVER_ENABLE_JOB_EXECUTION_ENV: str = "MLFLOW_SERVER_ENABLE_JOB_EXECUTION"

# Prefixes/exact names allow-listed for load_repo_root_env_fallback(): the
# repo-root .env holds many unrelated secrets for other apps, so only these
# are ever imported from it.
_ROOT_ENV_ALLOWED_PREFIXES: tuple[str, ...] = (
    "HORSE_RACING_MLFLOW_",
    "MLFLOW_",
    "R2_",
)
_ROOT_ENV_ALLOWED_EXACT: tuple[str, ...] = (_CLOUDFLARE_ACCOUNT_ID_ENV,)

_EXPORT_PREFIX_RE = re.compile(r"^export\s+")


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def _is_allowed_root_env_key(key: str) -> bool:
    if key in _ROOT_ENV_ALLOWED_EXACT:
        return True
    return any(key.startswith(prefix) for prefix in _ROOT_ENV_ALLOWED_PREFIXES)


def load_dotenv_local(env_file: Path | None = None) -> None:
    """Load KEY=VALUE pairs from a .env-style file into os.environ, without
    overriding any variable that is already set (explicit process env always
    wins). No-op if the file doesn't exist. Silently skips blank lines,
    lines starting with '#', and lines without an '=' sign. Strips one
    layer of matching single or double quotes from the value.

    This is a robustness fallback for non-interactive callers (launchd,
    cron, subprocess) that never source direnv's interactive-shell hook.
    ``env_file`` defaults to the file shared with the sibling ``mlflow``
    package at ``apps/mlflow/.env.local``.

    The explicit ``env_file`` argument always wins. When omitted, the
    HORSE_RACING_MLFLOW_ENV_FILE env var (if set) names the file to load
    instead of the default -- primarily so test suites can point this at a
    guaranteed-nonexistent path and make every call a no-op deterministically,
    without depending on whether the real .env.local exists on the machine
    running the tests.
    """
    if env_file is not None:
        path = env_file
    else:
        override = os.environ.get(_ENV_FILE_ENV, "").strip()
        path = Path(override) if override else DEFAULT_ENV_LOCAL_FILE
    if not path.is_file():
        return

    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, raw_value = line.partition("=")
        key = key.strip()
        if not key:
            continue
        value = _strip_quotes(raw_value.strip())
        os.environ.setdefault(key, value)


def load_repo_root_env_fallback(env_file: Path | None = None) -> None:
    """Fallback layer below load_dotenv_local(): parse specific keys directly
    out of the repo-root .env (NOT via direnv) so mlflow_ui resolves
    Neon/R2 config even when apps/mlflow/.env.local is missing or
    incomplete. Root .env holds many unrelated repo secrets, so only keys
    matching HORSE_RACING_MLFLOW_*/MLFLOW_*/R2_* or exactly
    CLOUDFLARE_ACCOUNT_ID are ever imported. Uses os.environ.setdefault, so
    any var already set (by the process env OR an earlier
    load_dotenv_local() call) always wins. Tolerates a leading 'export '
    prefix on a line. Never raises on a missing file or malformed line.

    The explicit ``env_file`` argument always wins. When omitted, the
    HORSE_RACING_MLFLOW_ROOT_ENV_FILE env var (if set) names the file to load
    instead of the default -- same rationale as load_dotenv_local()'s
    HORSE_RACING_MLFLOW_ENV_FILE override.
    """
    if env_file is not None:
        path = env_file
    else:
        override = os.environ.get(_ROOT_ENV_FILE_ENV, "").strip()
        path = Path(override) if override else DEFAULT_ROOT_ENV_FILE
    if not path.is_file():
        return

    for raw_line in path.read_text().splitlines():
        line = _EXPORT_PREFIX_RE.sub("", raw_line.strip())
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, raw_value = line.partition("=")
        key = key.strip()
        if not key or not _is_allowed_root_env_key(key):
            continue
        value = _strip_quotes(raw_value.strip())
        os.environ.setdefault(key, value)


@dataclass(frozen=True)
class Config:
    """Resolved runtime configuration for the mlflow-ui launcher."""

    data_dir: Path
    backend_store_uri: str
    artifacts_destination: str
    host: str
    port: int
    artifacts_mode: ArtifactsMode
    r2_account_id: str | None = field(default=None)
    r2_access_key_id: str | None = field(default=None)
    r2_secret_access_key: str | None = field(default=None)
    r2_bucket: str | None = field(default=None)
    r2_prefix: str = DEFAULT_R2_PREFIX


def _read_port(raw: str) -> int:
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(
            f"{_PORT_ENV}={raw!r} is not a valid integer port number"
        ) from exc


def _read_r2_account_id() -> str:
    value = os.environ.get(_R2_ACCOUNT_ID_ENV, "").strip()
    if value:
        return value
    return os.environ.get(_CLOUDFLARE_ACCOUNT_ID_ENV, "").strip()


def _require_r2_settings() -> tuple[str, str, str, str]:
    account_id = _read_r2_account_id()
    access_key_id = os.environ.get(_R2_ACCESS_KEY_ID_ENV, "").strip()
    secret_access_key = os.environ.get(_R2_SECRET_ACCESS_KEY_ENV, "").strip()
    bucket = os.environ.get(_R2_BUCKET_ENV, "").strip()

    missing: list[str] = []
    if not account_id:
        missing.append(f"{_R2_ACCOUNT_ID_ENV} (or {_CLOUDFLARE_ACCOUNT_ID_ENV})")
    if not access_key_id:
        missing.append(_R2_ACCESS_KEY_ID_ENV)
    if not secret_access_key:
        missing.append(_R2_SECRET_ACCESS_KEY_ENV)
    if not bucket:
        missing.append(_R2_BUCKET_ENV)

    if missing:
        joined = ", ".join(missing)
        raise ValueError(
            f"artifacts mode is 'r2' but the following env var(s) are missing "
            f"or empty: {joined}"
        )

    return account_id, access_key_id, secret_access_key, bucket


def load_config() -> Config:
    """Read all env vars and build an immutable :class:`Config`.

    Raises ``ValueError`` for an unrecognized artifacts mode, an invalid port
    string, or (in r2 mode) any missing required R2 credential/bucket var.
    """
    data_dir = Path(os.environ.get(_DATA_DIR_ENV, "") or str(DEFAULT_DATA_DIR))

    host = os.environ.get(_HOST_ENV, "") or DEFAULT_HOST
    port_raw = os.environ.get(_PORT_ENV, "")
    port = _read_port(port_raw) if port_raw else DEFAULT_PORT

    mode_raw = os.environ.get(_ARTIFACTS_MODE_ENV, "") or "local"
    artifacts_mode: ArtifactsMode
    if mode_raw == "local":
        artifacts_mode = "local"
    elif mode_raw == "r2":
        artifacts_mode = "r2"
    else:
        raise ValueError(
            f"{_ARTIFACTS_MODE_ENV}={mode_raw!r} is invalid; must be 'local' or 'r2'"
        )

    default_backend_uri = f"sqlite:///{data_dir / 'mlflow.db'}"
    backend_store_uri = os.environ.get(_BACKEND_URI_ENV, "") or default_backend_uri

    r2_account_id: str | None = None
    r2_access_key_id: str | None = None
    r2_secret_access_key: str | None = None
    r2_bucket: str | None = None
    r2_prefix = os.environ.get(_R2_PREFIX_ENV, "") or DEFAULT_R2_PREFIX

    if artifacts_mode == "r2":
        r2_account_id, r2_access_key_id, r2_secret_access_key, r2_bucket = (
            _require_r2_settings()
        )
        artifacts_destination = f"s3://{r2_bucket}/{r2_prefix}"
    else:
        artifacts_destination = f"file://{data_dir / 'mlartifacts'}"

    return Config(
        data_dir=data_dir,
        backend_store_uri=backend_store_uri,
        artifacts_destination=artifacts_destination,
        host=host,
        port=port,
        artifacts_mode=artifacts_mode,
        r2_account_id=r2_account_id,
        r2_access_key_id=r2_access_key_id,
        r2_secret_access_key=r2_secret_access_key,
        r2_bucket=r2_bucket,
        r2_prefix=r2_prefix,
    )


def server_env(cfg: Config) -> dict[str, str]:
    """Build the extra env vars to merge in for the ``mlflow server`` subprocess.

    Unconditionally disables mlflow's huey-based background job schedulers
    (``online_scoring_scheduler`` / ``trace_archival_scheduler``), which
    otherwise poll the backend database every minute and prevent Neon's
    serverless compute from ever auto-suspending. This repo uses none of
    mlflow's GenAI online-scoring or trace-archival features, so the
    subsystem has zero functional cost to disable, in either local or r2
    artifacts mode -- it is a backend-store/Postgres concern, unrelated to
    artifacts. An operator who deliberately sets
    MLFLOW_SERVER_ENABLE_JOB_EXECUTION themselves is never overridden.

    In local mode no other extra env is needed. In r2 mode this additionally
    points mlflow's boto3 client at the R2 S3-compatible endpoint, without
    clobbering AWS_* vars an operator may have deliberately set already.
    """
    env: dict[str, str] = {}
    if _MLFLOW_SERVER_ENABLE_JOB_EXECUTION_ENV not in os.environ:
        env[_MLFLOW_SERVER_ENABLE_JOB_EXECUTION_ENV] = "false"

    if cfg.artifacts_mode != "r2":
        return env

    account_id = cfg.r2_account_id or ""
    env[_MLFLOW_S3_ENDPOINT_URL_ENV] = f"https://{account_id}.r2.cloudflarestorage.com"
    env[_AWS_DEFAULT_REGION_ENV] = "auto"
    if _AWS_ACCESS_KEY_ID_ENV not in os.environ:
        env[_AWS_ACCESS_KEY_ID_ENV] = cfg.r2_access_key_id or ""
    if _AWS_SECRET_ACCESS_KEY_ENV not in os.environ:
        env[_AWS_SECRET_ACCESS_KEY_ENV] = cfg.r2_secret_access_key or ""
    return env


def ensure_wal(db_path: Path) -> None:
    """Ensure the sqlite backend DB uses WAL journal mode. Idempotent."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.commit()
    finally:
        conn.close()
