"""Tests for mlflow_ui.cli."""

from __future__ import annotations

from pathlib import Path

import pytest

from mlflow_ui import cli
from mlflow_ui.config import Config
from mlflow_ui.server import StartResult, StatusResult, StopResult


def _make_cfg(tmp_path: Path) -> Config:
    data_dir = tmp_path / "data"
    return Config(
        data_dir=data_dir,
        backend_store_uri=f"sqlite:///{data_dir / 'mlflow.db'}",
        artifacts_destination=f"file://{data_dir / 'mlartifacts'}",
        host="127.0.0.1",
        port=5252,
        artifacts_mode="local",
    )


def _stub_load_config(monkeypatch: pytest.MonkeyPatch, cfg: Config) -> None:
    monkeypatch.setattr(cli, "load_config", lambda: cfg)


@pytest.fixture(autouse=True)
def stub_env_file_loaders(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prevent every test in this module from touching the real
    apps/mlflow/.env.local or repo-root .env files -- both may hold real
    secrets on a developer machine, and cli.main() calls both loaders before
    anything else. Tests that specifically want to observe the call (e.g.
    test_main_loads_dotenv_local_and_repo_root_fallback_before_dispatch)
    re-stub with their own fake in the test body, which simply takes
    precedence over this one.
    """
    monkeypatch.setattr(cli, "load_dotenv_local", lambda: None)
    monkeypatch.setattr(cli, "load_repo_root_env_fallback", lambda: None)


def test_start_dispatch_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = _make_cfg(tmp_path)
    _stub_load_config(monkeypatch, cfg)
    monkeypatch.setattr(
        cli.server,
        "start",
        lambda passed_cfg: StartResult(ok=True, pid=42, url="http://127.0.0.1:5252", message="ok"),
    )

    exit_code = cli.main(["start"])

    assert exit_code == cli.EXIT_OK
    assert "ok" in capsys.readouterr().out


def test_start_dispatch_already_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _make_cfg(tmp_path)
    _stub_load_config(monkeypatch, cfg)
    monkeypatch.setattr(
        cli.server,
        "start",
        lambda passed_cfg: StartResult(
            ok=False, pid=1, url="http://127.0.0.1:5252", message="already running"
        ),
    )

    exit_code = cli.main(["start"])

    assert exit_code == cli.EXIT_ERROR


def test_start_dispatch_config_error_surfaces_as_exit_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ARTIFACTS_MODE", "r2")
    # bucket/account/credentials all left unset -> load_config() raises.

    exit_code = cli.main(["start"])

    assert exit_code == cli.EXIT_ERROR
    assert "error:" in capsys.readouterr().out


def test_stop_dispatch_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = _make_cfg(tmp_path)
    _stub_load_config(monkeypatch, cfg)
    monkeypatch.setattr(
        cli.server, "stop", lambda passed_cfg: StopResult(ok=True, message="stopped")
    )

    exit_code = cli.main(["stop"])

    assert exit_code == cli.EXIT_OK
    assert "stopped" in capsys.readouterr().out


def test_stop_dispatch_timeout_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _make_cfg(tmp_path)
    _stub_load_config(monkeypatch, cfg)
    monkeypatch.setattr(
        cli.server,
        "stop",
        lambda passed_cfg: StopResult(ok=False, message="did not stop in time"),
    )

    exit_code = cli.main(["stop"])

    assert exit_code == cli.EXIT_ERROR


def test_status_dispatch_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = _make_cfg(tmp_path)
    _stub_load_config(monkeypatch, cfg)
    monkeypatch.setattr(
        cli.server,
        "status",
        lambda passed_cfg: StatusResult(running=True, pid=42, url="http://127.0.0.1:5252"),
    )

    exit_code = cli.main(["status"])

    out = capsys.readouterr().out
    assert exit_code == cli.EXIT_OK
    assert "running" in out
    assert "42" in out


def test_status_dispatch_not_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = _make_cfg(tmp_path)
    _stub_load_config(monkeypatch, cfg)
    monkeypatch.setattr(
        cli.server,
        "status",
        lambda passed_cfg: StatusResult(running=False, pid=None, url="http://127.0.0.1:5252"),
    )

    exit_code = cli.main(["status"])

    out = capsys.readouterr().out
    assert exit_code == cli.EXIT_OK
    assert "not running" in out


def test_foreground_dispatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _make_cfg(tmp_path)
    _stub_load_config(monkeypatch, cfg)
    calls: list[Config] = []
    monkeypatch.setattr(cli.server, "run_foreground", calls.append)

    exit_code = cli.main(["foreground"])

    assert exit_code == cli.EXIT_OK
    assert calls == [cfg]


def test_plist_dispatch_prints_to_stdout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = _make_cfg(tmp_path)
    _stub_load_config(monkeypatch, cfg)
    monkeypatch.setattr(cli, "generate_plist", lambda passed_cfg: "<plist-text/>")

    exit_code = cli.main(["plist"])

    assert exit_code == cli.EXIT_OK
    assert "<plist-text/>" in capsys.readouterr().out


def test_plist_dispatch_writes_output_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _make_cfg(tmp_path)
    _stub_load_config(monkeypatch, cfg)
    monkeypatch.setattr(cli, "generate_plist", lambda passed_cfg: "<plist-text/>")
    output_path = tmp_path / "out.plist"

    exit_code = cli.main(["plist", "--output", str(output_path)])

    assert exit_code == cli.EXIT_OK
    assert output_path.read_text() == "<plist-text/>"


def test_main_requires_a_subcommand() -> None:
    with pytest.raises(SystemExit):
        cli.main([])


def test_main_loads_dotenv_local_before_dispatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _make_cfg(tmp_path)
    calls: list[str] = []

    def _fake_load_dotenv_local() -> None:
        calls.append("load_dotenv_local")
        # Loader must run before load_config() so it can populate os.environ
        # in time for load_config() to observe the vars it sets.
        assert cli.load_config is not None

    def _fake_load_repo_root_env_fallback() -> None:
        calls.append("load_repo_root_env_fallback")

    monkeypatch.setattr(cli, "load_dotenv_local", _fake_load_dotenv_local)
    monkeypatch.setattr(cli, "load_repo_root_env_fallback", _fake_load_repo_root_env_fallback)
    _stub_load_config(monkeypatch, cfg)
    monkeypatch.setattr(
        cli.server,
        "status",
        lambda passed_cfg: StatusResult(running=False, pid=None, url="http://127.0.0.1:5252"),
    )

    exit_code = cli.main(["status"])

    assert exit_code == cli.EXIT_OK
    # Order matters: load_dotenv_local must run first so
    # load_repo_root_env_fallback's setdefault calls layer beneath it
    # (precedence: process env > .env.local > root .env fallback).
    assert calls == ["load_dotenv_local", "load_repo_root_env_fallback"]
