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
    curl = binary / "curl"
    curl.write_text(
        '#!/bin/bash\nprintf "%s\\n" "$*" >> "$COMMAND_LOG"\nprintf "%s" "$HEALTH_RESPONSE"\n',
        encoding="utf-8",
    )
    curl.chmod(0o755)
    git = binary / "git"
    git.write_text(
        "#!/bin/bash\n"
        'if [[ "${ADVANCE_MAIN:-}" == true && '
        '"$(< "$COMMAND_LOG")" == *"venue-weather deploy"* ]]; '
        'then echo newer; else printf "%s\\trefs/heads/main\\n" "$REMOTE_SHA"; fi\n',
        encoding="utf-8",
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
    (tmp_path / "apps" / "daily-keiba-sync").mkdir()
    (tmp_path / "apps" / "pc-keiba-viewer").mkdir()
    (tmp_path / "apps" / "mlflow-ui-proxy").mkdir()
    (tmp_path / "apps" / "jra-van-datalab-worker-only-probe").mkdir()
    (tmp_path / "apps" / "jra-van-datalab-cloudflare-demo").mkdir()
    (tmp_path / "scripts").mkdir()
    wrapper = tmp_path / "scripts" / "ensure-docker-compat.sh"
    wrapper.write_text('#!/bin/bash\nshift\nexec "$@"\n', encoding="utf-8")
    wrapper.chmod(0o755)
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


def test_main_advancing_between_services_stops_remaining_deployments(
    runner, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ADVANCE_MAIN", "true")
    run, _targets = runner
    code, log = run("false")
    assert code == 1
    assert "venue-weather deploy" in log
    assert "pipeline-health-monitor deploy" not in log


def test_validation_failure_prevents_every_deployment(
    runner, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAIL_COMMAND", "pipeline-health-monitor test:coverage")
    run, _targets = runner
    code, log = run("false")
    assert code == 9
    assert " deploy\n" not in log


def test_validate_only_never_mutates_production(runner) -> None:
    run, _targets = runner
    code, log = run("true")
    assert code == 0
    assert " deploy\n" not in log
    assert "test:coverage" in log


def test_stale_checkout_cannot_deploy(runner, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REMOTE_SHA", "newer")
    run, _targets = runner
    code, log = run("false")
    assert code == 1
    assert " deploy\n" not in log


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


def test_viewer_python_gates_are_required_even_for_validate_only(runner) -> None:
    run, targets = runner
    targets.write_text('["pc-keiba-viewer"]', encoding="utf-8")
    code, log = run("true")
    assert code == 0
    assert "run --filter pc-keiba-viewer python:check\n" in log
    assert log.index("next typegen\n") < log.index("pc-keiba-viewer tsc\n")


def test_mlflow_python_gates_are_required_for_container_changes(runner) -> None:
    run, targets = runner
    targets.write_text('["mlflow-ui-proxy"]', encoding="utf-8")
    code, log = run("true")
    assert code == 0
    assert "run --filter mlflow python:check\n" in log


def test_private_core_compatibility_is_checked(runner) -> None:
    run, targets = runner
    targets.write_text('["jra-van-datalab-worker-only-probe"]', encoding="utf-8")
    code, log = run("true")
    assert code == 0
    assert "run --filter jra-van-datalab-worker-only-probe core:prepare\n" in log
    assert "run --filter jra-van-datalab-worker-only-probe test:compatibility:local\n" in log


@pytest.mark.parametrize(
    ("response", "expected_code"),
    [
        ('{"ok":true,"runtime":"cloudflare-workers-native-daily-keiba-sync"}', 0),
        ('{"ok":false}', 1),
    ],
)
def test_daily_sync_requires_healthy_runtime(
    runner, monkeypatch: pytest.MonkeyPatch, response: str, expected_code: int
) -> None:
    monkeypatch.setenv("HEALTH_RESPONSE", response)
    run, targets = runner
    targets.write_text('["daily-keiba-sync"]', encoding="utf-8")
    code, log = run("false")
    assert code == expected_code
    assert "https://daily-keiba-sync.kaoru.workers.dev/health" in log


def test_failed_build_prevents_any_production_deployment(
    runner, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAIL_COMMAND", "wrangler deploy --dry-run")
    run, _targets = runner
    code, log = run("false")
    assert code == 9
    assert " deploy\n" not in log


def test_viewer_validate_only_builds_without_publishing(runner) -> None:
    run, targets = runner
    targets.write_text('["pc-keiba-viewer"]', encoding="utf-8")
    code, log = run("true")
    assert code == 0
    assert log.index("opennextjs-cloudflare build\n") < log.index("wrangler deploy --dry-run\n")
    assert " deploy\n" not in log


def test_wine_container_restores_private_sdk_before_building(runner) -> None:
    run, targets = runner
    targets.write_text('["jra-van-datalab-cloudflare-demo"]', encoding="utf-8")
    code, log = run("true")
    assert code == 0
    assert log.index("python .github/deploy/sdk.py\n") < log.index("wrangler deploy --dry-run\n")
