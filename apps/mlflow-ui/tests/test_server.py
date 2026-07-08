"""Tests for mlflow_ui.server."""

from __future__ import annotations

import signal
from pathlib import Path

import pytest

from mlflow_ui import server
from mlflow_ui.config import Config


def _make_local_cfg(tmp_path: Path, *, port: int = 5252) -> Config:
    data_dir = tmp_path / "data"
    return Config(
        data_dir=data_dir,
        backend_store_uri=f"sqlite:///{data_dir / 'mlflow.db'}",
        artifacts_destination=f"file://{data_dir / 'mlartifacts'}",
        host="127.0.0.1",
        port=port,
        artifacts_mode="local",
    )


def _make_r2_cfg(tmp_path: Path) -> Config:
    data_dir = tmp_path / "data"
    return Config(
        data_dir=data_dir,
        backend_store_uri=f"sqlite:///{data_dir / 'mlflow.db'}",
        artifacts_destination="s3://my-bucket/mlflow",
        host="127.0.0.1",
        port=5252,
        artifacts_mode="r2",
        r2_account_id="acct123",
        r2_access_key_id="key123",
        r2_secret_access_key="secret123",
        r2_bucket="my-bucket",
    )


def test_build_command(tmp_path: Path) -> None:
    cfg = _make_local_cfg(tmp_path, port=6000)

    assert server.build_command(cfg) == [
        "mlflow",
        "server",
        "--host",
        "127.0.0.1",
        "--port",
        "6000",
    ]


def test_build_command_never_puts_backend_uri_or_artifacts_destination_in_argv(
    tmp_path: Path,
) -> None:
    """Regression guard for the ps-argv DSN leak: build_command() must never
    surface --backend-store-uri/--artifacts-destination or their values --
    both reach mlflow server exclusively via config.server_env()."""
    cfg = _make_local_cfg(tmp_path)

    command = server.build_command(cfg)

    assert "--backend-store-uri" not in command
    assert "--artifacts-destination" not in command
    assert cfg.backend_store_uri not in command
    assert cfg.artifacts_destination not in command


def test_server_url(tmp_path: Path) -> None:
    cfg = _make_local_cfg(tmp_path, port=6000)

    assert server.server_url(cfg) == "http://127.0.0.1:6000"


def test_pid_and_log_file_paths(tmp_path: Path) -> None:
    cfg = _make_local_cfg(tmp_path)

    assert server.pid_file_path(cfg) == cfg.data_dir / "mlflow-ui.pid"
    assert server.log_file_path(cfg) == cfg.data_dir / "mlflow-ui.log"


def test_is_running_true_when_process_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server.os, "kill", lambda pid, sig: None)

    assert server.is_running(4242) is True


def test_is_running_false_when_process_lookup_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_kill(pid: int, sig: int) -> None:
        raise ProcessLookupError

    monkeypatch.setattr(server.os, "kill", fake_kill)

    assert server.is_running(4242) is False


def test_is_running_true_when_permission_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_kill(pid: int, sig: int) -> None:
        raise PermissionError

    monkeypatch.setattr(server.os, "kill", fake_kill)

    assert server.is_running(4242) is True


class _FakePopen:
    def __init__(self, args: list[str], **kwargs: object) -> None:
        self.args: list[str] = args
        self.kwargs: dict[str, object] = kwargs
        self.pid: int = 9876


def test_start_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _make_local_cfg(tmp_path)
    monkeypatch.setattr(server.subprocess, "Popen", _FakePopen)

    result = server.start(cfg)

    assert result.ok is True
    assert result.pid == 9876
    assert result.url == "http://127.0.0.1:5252"
    pid_path = server.pid_file_path(cfg)
    assert pid_path.read_text() == "9876"
    assert server.log_file_path(cfg).exists()
    assert (cfg.data_dir / "mlartifacts").is_dir()


def test_start_passes_backend_uri_and_artifacts_destination_via_env_not_argv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _make_local_cfg(tmp_path)
    recorded_calls: list[tuple[list[str], dict[str, str]]] = []

    def fake_popen(
        args: list[str], *, env: dict[str, str], **kwargs: object
    ) -> _FakePopen:
        recorded_calls.append((args, env))
        return _FakePopen(args, env=env, **kwargs)

    monkeypatch.setattr(server.subprocess, "Popen", fake_popen)

    server.start(cfg)

    assert len(recorded_calls) == 1
    args, env = recorded_calls[0]
    assert "--backend-store-uri" not in args
    assert "--artifacts-destination" not in args
    assert cfg.backend_store_uri not in args
    assert cfg.artifacts_destination not in args
    assert env["MLFLOW_BACKEND_STORE_URI"] == cfg.backend_store_uri
    assert env["MLFLOW_ARTIFACTS_DESTINATION"] == cfg.artifacts_destination


def test_start_refuses_when_already_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _make_local_cfg(tmp_path)
    cfg.data_dir.mkdir(parents=True)
    server.pid_file_path(cfg).write_text("1111")
    monkeypatch.setattr(server.os, "kill", lambda pid, sig: None)

    def fail_popen(*_args: object, **_kwargs: object) -> _FakePopen:
        raise AssertionError("Popen should not be called when already running")

    monkeypatch.setattr(server.subprocess, "Popen", fail_popen)

    result = server.start(cfg)

    assert result.ok is False
    assert result.pid == 1111
    assert "already running" in result.message


def test_start_cleans_up_stale_pid_file_and_proceeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _make_local_cfg(tmp_path)
    cfg.data_dir.mkdir(parents=True)
    server.pid_file_path(cfg).write_text("2222")

    def fake_kill(pid: int, sig: int) -> None:
        raise ProcessLookupError

    monkeypatch.setattr(server.os, "kill", fake_kill)
    monkeypatch.setattr(server.subprocess, "Popen", _FakePopen)

    result = server.start(cfg)

    assert result.ok is True
    assert result.pid == 9876
    assert server.pid_file_path(cfg).read_text() == "9876"


def test_stop_not_running_when_no_pid_file(tmp_path: Path) -> None:
    cfg = _make_local_cfg(tmp_path)

    result = server.stop(cfg, sleep_fn=lambda _seconds: None)

    assert result.ok is True
    assert "not running" in result.message


def test_stop_cleans_up_stale_pid_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _make_local_cfg(tmp_path)
    cfg.data_dir.mkdir(parents=True)
    pid_path = server.pid_file_path(cfg)
    pid_path.write_text("3333")

    def fake_kill(pid: int, sig: int) -> None:
        raise ProcessLookupError

    monkeypatch.setattr(server.os, "kill", fake_kill)

    result = server.stop(cfg, sleep_fn=lambda _seconds: None)

    assert result.ok is True
    assert "not running" in result.message
    assert not pid_path.exists()


def test_stop_sends_sigterm_and_confirms_death(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _make_local_cfg(tmp_path)
    cfg.data_dir.mkdir(parents=True)
    pid_path = server.pid_file_path(cfg)
    pid_path.write_text("4444")

    kill_calls: list[tuple[int, int]] = []
    running_sequence = iter([True, True, False])

    def fake_kill(pid: int, sig: int) -> None:
        kill_calls.append((pid, sig))
        if sig == 0:
            if not next(running_sequence):
                raise ProcessLookupError
            return
        assert sig == signal.SIGTERM

    monkeypatch.setattr(server.os, "kill", fake_kill)

    sleeps: list[float] = []
    result = server.stop(cfg, sleep_fn=sleeps.append)

    assert result.ok is True
    assert "stopped" in result.message
    assert not pid_path.exists()
    assert (4444, signal.SIGTERM) in kill_calls
    assert len(sleeps) == 1


def test_stop_reports_timeout_without_sigkill(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _make_local_cfg(tmp_path)
    cfg.data_dir.mkdir(parents=True)
    pid_path = server.pid_file_path(cfg)
    pid_path.write_text("5555")

    kill_calls: list[tuple[int, int]] = []

    def fake_kill(pid: int, sig: int) -> None:
        kill_calls.append((pid, sig))
        # Always "alive": no ProcessLookupError raised.

    monkeypatch.setattr(server.os, "kill", fake_kill)

    sleeps: list[float] = []
    result = server.stop(cfg, sleep_fn=sleeps.append)

    assert result.ok is False
    assert "did not stop" in result.message
    assert pid_path.exists()
    signals_sent = {sig for _pid, sig in kill_calls}
    assert signal.SIGKILL not in signals_sent
    # The exact retry budget is an internal implementation detail; assert it
    # polled a bounded, positive number of times rather than importing the
    # module-private constant.
    assert 0 < len(sleeps) < 1000


def test_status_running(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _make_local_cfg(tmp_path)
    cfg.data_dir.mkdir(parents=True)
    server.pid_file_path(cfg).write_text("6666")
    monkeypatch.setattr(server.os, "kill", lambda pid, sig: None)

    result = server.status(cfg)

    assert result.running is True
    assert result.pid == 6666
    assert result.url == "http://127.0.0.1:5252"


def test_status_not_running_no_pid_file(tmp_path: Path) -> None:
    cfg = _make_local_cfg(tmp_path)

    result = server.status(cfg)

    assert result.running is False
    assert result.pid is None


def test_status_not_running_stale_pid_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _make_local_cfg(tmp_path)
    cfg.data_dir.mkdir(parents=True)
    server.pid_file_path(cfg).write_text("7777")

    def fake_kill(pid: int, sig: int) -> None:
        raise ProcessLookupError

    monkeypatch.setattr(server.os, "kill", fake_kill)

    result = server.status(cfg)

    assert result.running is False
    assert result.pid is None


def test_run_foreground_execs_with_merged_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _make_r2_cfg(tmp_path)
    recorded_calls: list[tuple[str, list[str], dict[str, str]]] = []

    def fake_execvpe(file: str, args: list[str], env: dict[str, str]) -> None:
        recorded_calls.append((file, args, env))

    monkeypatch.setattr(server.os, "execvpe", fake_execvpe)

    server.run_foreground(cfg)

    assert len(recorded_calls) == 1
    file, args, env = recorded_calls[0]
    assert file == "mlflow"
    assert args == server.build_command(cfg)
    assert "--backend-store-uri" not in args
    assert "--artifacts-destination" not in args
    assert env["MLFLOW_S3_ENDPOINT_URL"] == "https://acct123.r2.cloudflarestorage.com"
    assert env["MLFLOW_BACKEND_STORE_URI"] == cfg.backend_store_uri
    assert env["MLFLOW_ARTIFACTS_DESTINATION"] == cfg.artifacts_destination
    assert cfg.data_dir.is_dir()
    assert not (cfg.data_dir / "mlartifacts").exists()
    db_path = cfg.data_dir / "mlflow.db"
    assert db_path.exists()
