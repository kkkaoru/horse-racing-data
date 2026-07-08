"""Process lifecycle management for the ``mlflow server`` subprocess.

PID-file + SIGTERM only -- never SIGKILL, never kill-by-name. Identity is
always verified against the recorded PID before any signal is sent.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from mlflow_ui.config import Config, ensure_wal, server_env

PID_FILENAME: str = "mlflow-ui.pid"
LOG_FILENAME: str = "mlflow-ui.log"

_STOP_MAX_ATTEMPTS: int = 20
_STOP_POLL_INTERVAL_SECONDS: float = 0.5


@dataclass(frozen=True)
class StartResult:
    """Outcome of a :func:`start` call."""

    ok: bool
    pid: int | None
    url: str
    message: str


@dataclass(frozen=True)
class StopResult:
    """Outcome of a :func:`stop` call."""

    ok: bool
    message: str


@dataclass(frozen=True)
class StatusResult:
    """Outcome of a :func:`status` call."""

    running: bool
    pid: int | None
    url: str


def pid_file_path(cfg: Config) -> Path:
    return cfg.data_dir / PID_FILENAME


def log_file_path(cfg: Config) -> Path:
    return cfg.data_dir / LOG_FILENAME


def server_url(cfg: Config) -> str:
    return f"http://{cfg.host}:{cfg.port}"


def build_command(cfg: Config) -> list[str]:
    """Build the ``mlflow server`` argv for this config."""
    return [
        "mlflow",
        "server",
        "--backend-store-uri",
        cfg.backend_store_uri,
        "--artifacts-destination",
        cfg.artifacts_destination,
        "--host",
        cfg.host,
        "--port",
        str(cfg.port),
    ]


def is_running(pid: int) -> bool:
    """Return True if a process with the given PID exists (regardless of owner)."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _read_pid_file(pid_path: Path) -> int | None:
    if not pid_path.exists():
        return None
    return int(pid_path.read_text().strip())


def _clear_stale_pid_file(pid_path: Path) -> None:
    pid_path.unlink(missing_ok=True)


def _ensure_dirs(cfg: Config) -> None:
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    if cfg.artifacts_mode == "local":
        (cfg.data_dir / "mlartifacts").mkdir(parents=True, exist_ok=True)


def _prepare_start(cfg: Config) -> tuple[Path, str] | StartResult:
    """Shared pre-flight for start(): returns either (pid_path, url) to
    proceed with, or a StartResult to return immediately (already running)."""
    pid_path = pid_file_path(cfg)
    url = server_url(cfg)
    existing_pid = _read_pid_file(pid_path)
    if existing_pid is not None:
        if is_running(existing_pid):
            return StartResult(
                ok=False,
                pid=existing_pid,
                url=url,
                message=f"mlflow-ui is already running (pid {existing_pid})",
            )
        _clear_stale_pid_file(pid_path)
    return pid_path, url


def start(cfg: Config) -> StartResult:
    """Start ``mlflow server`` as a detached background subprocess."""
    prepared = _prepare_start(cfg)
    if isinstance(prepared, StartResult):
        return prepared
    pid_path, url = prepared

    _ensure_dirs(cfg)
    db_path = cfg.data_dir / "mlflow.db"
    ensure_wal(db_path)

    merged_env = os.environ | server_env(cfg)
    command = build_command(cfg)
    log_path = log_file_path(cfg)
    logfile = log_path.open("a")
    try:
        process = subprocess.Popen(
            command,
            stdout=logfile,
            stderr=logfile,
            env=merged_env,
        )
    finally:
        logfile.close()

    pid_path.write_text(str(process.pid))
    return StartResult(
        ok=True,
        pid=process.pid,
        url=url,
        message=f"mlflow-ui started (pid {process.pid}) at {url}",
    )


def stop(
    cfg: Config,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> StopResult:
    """Stop a running ``mlflow server`` via SIGTERM, polling for exit."""
    pid_path = pid_file_path(cfg)
    pid = _read_pid_file(pid_path)
    if pid is None:
        return StopResult(ok=True, message="mlflow-ui is not running")

    if not is_running(pid):
        _clear_stale_pid_file(pid_path)
        return StopResult(ok=True, message="mlflow-ui is not running")

    os.kill(pid, signal.SIGTERM)
    for _ in range(_STOP_MAX_ATTEMPTS):
        if not is_running(pid):
            _clear_stale_pid_file(pid_path)
            return StopResult(ok=True, message=f"mlflow-ui (pid {pid}) stopped")
        sleep_fn(_STOP_POLL_INTERVAL_SECONDS)

    return StopResult(
        ok=False,
        message=f"mlflow-ui (pid {pid}) did not stop within the timeout",
    )


def status(cfg: Config) -> StatusResult:
    """Report whether ``mlflow server`` is currently running."""
    pid_path = pid_file_path(cfg)
    pid = _read_pid_file(pid_path)
    url = server_url(cfg)
    if pid is not None and is_running(pid):
        return StatusResult(running=True, pid=pid, url=url)
    return StatusResult(running=False, pid=None, url=url)


def run_foreground(cfg: Config) -> None:
    """Run ``mlflow server`` in the foreground, replacing this process.

    Intended for supervision by launchd (``KeepAlive``): no wrapper
    subprocess is kept around to manage.
    """
    _ensure_dirs(cfg)
    db_path = cfg.data_dir / "mlflow.db"
    ensure_wal(db_path)

    merged_env = os.environ | server_env(cfg)
    command = build_command(cfg)
    os.execvpe(command[0], command, merged_env)
