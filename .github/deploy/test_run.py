"""Exercise the deployment shell with fake CLIs; never contact production."""

import json
import os
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).with_name("run.sh").resolve()


@pytest.fixture
def runner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    binary = tmp_path / "bin"
    binary.mkdir()
    fake = binary / "bun"
    fake.write_text(
        '#!/bin/bash\nprintf "%s\\n" "$*" >> "$COMMAND_LOG"\n'
        'if [[ -n "${FAIL_COMMAND:-}" && "$*" == *"$FAIL_COMMAND"* ]]; then exit 9; fi\n',
        encoding="utf-8",
    )
    fake.chmod(0o755)
    (binary / "bunx").symlink_to(fake)
    (binary / "uv").symlink_to(fake)
    git = binary / "git"
    git.write_text(
        '#!/bin/bash\nprintf "%s\\trefs/heads/main\\n" "$REMOTE_SHA"\n', encoding="utf-8"
    )
    git.chmod(0o755)
    log = tmp_path / "commands"
    monkeypatch.setenv("PATH", f"{binary}:{os.environ['PATH']}")
    monkeypatch.setenv("COMMAND_LOG", str(log))
    monkeypatch.setenv("GITHUB_SHA", "current")
    monkeypatch.setenv("REMOTE_SHA", "current")
    (tmp_path / "apps" / "venue-weather").mkdir(parents=True)
    (tmp_path / "apps" / "pipeline-health-monitor").mkdir()
    (tmp_path / "apps" / "finish-position-cron").mkdir()
    targets = tmp_path / "targets.json"
    targets.write_text(json.dumps(["venue-weather", "pipeline-health-monitor"]), encoding="utf-8")

    def run(validate_only: str) -> tuple[int, str]:
        result = subprocess.run(
            ["bash", str(SCRIPT), str(targets), validate_only],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            check=False,
        )
        return result.returncode, log.read_text(encoding="utf-8")

    return run, targets


def test_validation_failure_prevents_every_deployment(
    runner, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAIL_COMMAND", "pipeline-health-monitor test:coverage")
    run, _targets = runner
    code, log = run("false")
    assert code == 9
    assert " deploy" not in log


def test_validate_only_never_mutates_production(runner) -> None:
    run, _targets = runner
    code, log = run("true")
    assert code == 0
    assert " deploy" not in log
    assert "test:coverage" in log


def test_stale_checkout_cannot_deploy(runner, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REMOTE_SHA", "newer")
    run, _targets = runner
    code, log = run("false")
    assert code == 1
    assert " deploy" not in log


def test_deployment_failure_stops_subsequent_services(
    runner, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAIL_COMMAND", "venue-weather deploy")
    run, _targets = runner
    code, log = run("false")
    assert code == 9
    assert "pipeline-health-monitor deploy" not in log


def test_success_validates_before_deploying(runner) -> None:
    run, _targets = runner
    code, log = run("false")
    assert code == 0
    assert log.index("pipeline-health-monitor test:coverage") < log.index("venue-weather deploy")
    assert log.count("wrangler deployments list") == 2


def test_prediction_uses_the_existing_guarded_deploy_command(runner) -> None:
    run, targets = runner
    targets.write_text('["finish-position-cron"]', encoding="utf-8")
    code, log = run("false")
    assert code == 0
    assert "models.py --root apps/finish-position-predict-container/models" in log
    assert "artifact:verify -- --artifact-root models --system finish-position" in log
    assert "run --filter finish-position-cron deploy\n" in log
    assert "wrangler deploy\n" not in log
