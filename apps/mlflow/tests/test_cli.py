"""Tests for mlflow_tracking.cli.

The conftest.py `isolate_data_dir` autouse fixture already points
HORSE_RACING_MLFLOW_DATA_DIR at tmp_path for every test in this suite, and
unconditionally clears HORSE_RACING_MLFLOW_BACKEND_URI. cli.py's own
`build_client()` (which has no override parameter) relies on both so it
never touches the real repo's apps/mlflow/data directory or a real backend.

A handful of tests below build their own MlflowClient directly via
config.get_tracking_uri() (rather than through cli.main()) to pre-seed
registry state ahead of a CLI invocation that must see the same store.
`_isolated_tracking_uri()` is a belt-and-suspenders guard for those call
sites: see its docstring for the incident that motivates it.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from mlflow import MlflowClient

from mlflow_tracking import (
    backfill_finish_position,
    backfill_running_style,
    cli,
    config,
    export_production,
    registry,
)
from mlflow_tracking.backfill_running_style import RunningStyleBackfillSummary

WriteJsonFixture = Callable[[Path, object], None]


@pytest.fixture(autouse=True)
def clear_cli_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear ambient tracking/artifact env vars so each CLI test starts clean.

    Also stubs config.load_dotenv_local and config.load_repo_root_env_fallback
    to no-ops: cli.main() now calls both unconditionally on every invocation,
    and their real default env_file paths resolve to the actual
    apps/mlflow/.env.local and repo-root .env on disk (real, gitignored,
    secret-bearing files). Leaving them un-stubbed here would let the first
    test in this file that calls cli.main() load those real secrets into
    this process's os.environ, where they would persist (setdefault never
    unsets them) and leak into every later test.
    test_main_calls_load_dotenv_local_before_dispatch and
    test_main_calls_load_repo_root_env_fallback_after_dotenv_local below
    override these stubs to verify the wiring itself.
    """
    monkeypatch.delenv(config.ENV_TRACKING_URI, raising=False)
    monkeypatch.delenv(config.ENV_ARTIFACTS_MODE, raising=False)
    monkeypatch.delenv(config.ENV_R2_BUCKET, raising=False)
    monkeypatch.setattr(config, "load_dotenv_local", lambda: None)
    monkeypatch.setattr(config, "load_repo_root_env_fallback", lambda: None)


def _isolated_tracking_uri(tmp_path: Path) -> str:
    """Resolve config.get_tracking_uri() and hard-fail if it escapes tmp_path.

    Belt-and-suspenders for the 2026-07-08 incident (see
    conftest.clear_ambient_backend_uri's docstring): several tests in this
    file build a raw MlflowClient directly from config.get_tracking_uri() to
    pre-seed registry state ahead of a cli.main() call that must read the
    same store, rather than going through the isolated `client` fixture (a
    different, unrelated sqlite file that cli.main()'s own build_client()
    would never see). If the autouse env-clearing were ever bypassed,
    removed, or reordered, this raises loudly inside the test that would
    otherwise silently write to whatever real backend the URI resolves to.
    """
    uri = config.get_tracking_uri()
    prefix = "sqlite:///"
    assert uri.startswith(prefix), f"expected an isolated sqlite tracking URI, got: {uri!r}"
    db_path = Path(uri.removeprefix(prefix))
    assert db_path.is_relative_to(tmp_path.resolve()), (
        f"refusing to build an MlflowClient against a non-isolated tracking URI: {uri!r}"
    )
    return uri


def test_init_creates_data_dir_and_all_experiments(tmp_path: Path) -> None:
    exit_code = cli.main(["init"])
    assert exit_code == 0
    data_dir = tmp_path / "data"
    assert data_dir.is_dir()
    assert config.db_path_for(data_dir).is_file()
    client = MlflowClient(tracking_uri=_isolated_tracking_uri(tmp_path))
    for experiment_name in config.ALL_EXPERIMENT_NAMES:
        assert client.get_experiment_by_name(experiment_name) is not None


def test_init_warns_when_r2_bucket_unreachable(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv(config.ENV_ARTIFACTS_MODE, "r2")
    monkeypatch.setenv(config.ENV_R2_BUCKET, "unreachable-bucket")
    monkeypatch.setattr(config, "check_r2_bucket_reachable", lambda bucket: False)
    exit_code = cli.main(["init"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "unreachable-bucket" in captured.err


def test_init_does_not_warn_when_r2_bucket_reachable(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv(config.ENV_ARTIFACTS_MODE, "r2")
    monkeypatch.setenv(config.ENV_R2_BUCKET, "reachable-bucket")
    monkeypatch.setattr(config, "check_r2_bucket_reachable", lambda bucket: True)
    exit_code = cli.main(["init"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "not reachable" not in captured.err


def test_init_confines_default_experiment_artifact_root_to_data_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """mlflow auto-creates its own "Default" experiment (id=0) on a fresh
    store's first touch, with an artifact_location resolved against
    whatever the process cwd happens to be at that moment -- there is no
    public, non-server API to override this directly. `init` must confine
    that first touch to the data dir regardless of the caller's actual cwd
    (e.g. repo root), and must restore cwd afterward.
    """
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    exit_code = cli.main(["init"])

    assert exit_code == 0
    assert Path.cwd() == elsewhere
    client = MlflowClient(tracking_uri=_isolated_tracking_uri(tmp_path))
    default_experiment = client.get_experiment("0")
    location = default_experiment.artifact_location.removeprefix("file://")
    assert Path(location).is_relative_to(tmp_path / "data")


def test_cmd_backfill_finish_position_reports_success(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    summary = backfill_finish_position.BackfillSummary(
        base_versions_registered=2,
        per_class_versions_registered=1,
        errors=[],
        champion_sync={"jra": "ok"},
        cell_routing_run_id="abc123",
    )
    monkeypatch.setattr(
        backfill_finish_position, "backfill_finish_position", lambda client: summary
    )
    exit_code = cli.main(["backfill-finish-position"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "base versions registered: 2" in captured.out


def test_cmd_backfill_finish_position_reports_errors(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    summary = backfill_finish_position.BackfillSummary(
        base_versions_registered=0,
        per_class_versions_registered=0,
        errors=["bad file"],
        champion_sync={},
        cell_routing_run_id=None,
    )
    monkeypatch.setattr(
        backfill_finish_position, "backfill_finish_position", lambda client: summary
    )
    exit_code = cli.main(["backfill-finish-position"])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "error: bad file" in captured.err


def test_cmd_backfill_finish_position_fails_when_champion_sync_incomplete(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    summary = backfill_finish_position.BackfillSummary(
        base_versions_registered=1,
        per_class_versions_registered=0,
        errors=[],
        champion_sync={"jra": "skipped: no registered version tagged model_version=x"},
        cell_routing_run_id=None,
    )
    monkeypatch.setattr(
        backfill_finish_position, "backfill_finish_position", lambda client: summary
    )
    exit_code = cli.main(["backfill-finish-position"])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "warning: champion sync incomplete for 'jra'" in captured.err
    assert "--allow-missing-champion" in captured.err


def test_cmd_backfill_finish_position_allow_missing_champion_flag_succeeds(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    summary = backfill_finish_position.BackfillSummary(
        base_versions_registered=1,
        per_class_versions_registered=0,
        errors=[],
        champion_sync={"jra": "skipped: no registered version tagged model_version=x"},
        cell_routing_run_id=None,
    )
    monkeypatch.setattr(
        backfill_finish_position, "backfill_finish_position", lambda client: summary
    )
    exit_code = cli.main(["backfill-finish-position", "--allow-missing-champion"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "warning: champion sync incomplete for 'jra'" in captured.err


def test_cmd_backfill_finish_position_models_root_flag_uses_flat_backfill(
    tmp_path: Path, write_json: WriteJsonFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    flat_root = tmp_path / "flat-models"
    write_json(
        flat_root / "jra-cb-flat-v1" / "metadata.json",
        {"model_version": "jra-cb-flat-v1", "category": "jra", "architecture": "catboost"},
    )
    exit_code = cli.main(["backfill-finish-position", "--models-root", str(flat_root)])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "versions registered: 1" in captured.out
    assert "skipped (already registered): 0" in captured.out


def test_cmd_backfill_finish_position_models_root_flag_reports_errors(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    flat_root = tmp_path / "flat-models"
    bad_path = flat_root / "bad-version" / "metadata.json"
    bad_path.parent.mkdir(parents=True, exist_ok=True)
    bad_path.write_text("{not valid json", encoding="utf-8")
    exit_code = cli.main(["backfill-finish-position", "--models-root", str(flat_root)])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "error:" in captured.err


def test_cmd_backfill_running_style_reports_success(
    tmp_path: Path, write_json: WriteJsonFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    artifact_root = tmp_path / "rs-artifacts"
    write_json(
        artifact_root / "jra-running-style-lgbm-prod-v3" / "metadata.json",
        {
            "model_version": "jra-running-style-lgbm-prod-v3",
            "class_labels": ["nige", "senkou", "sashi", "oikomi"],
            "feature_columns": ["speed_index"],
        },
    )
    exit_code = cli.main(["backfill-running-style", str(artifact_root)])
    assert exit_code == 0
    captured = capsys.readouterr()
    # 1 from the local mirror (jra) + 1 production-pointer fallback (nar)
    assert "versions registered: 2" in captured.out
    assert "champion sync:" in captured.out


def test_cmd_backfill_running_style_uses_module_default_when_path_omitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_roots: list[Path] = []

    def _fake_backfill(
        client: MlflowClient, artifact_root: Path, calibrator_dir: Path = Path()
    ) -> RunningStyleBackfillSummary:
        seen_roots.append(artifact_root)
        return RunningStyleBackfillSummary(versions_registered=0, errors=[], champion_sync={})

    monkeypatch.setattr(backfill_running_style, "backfill_running_style", _fake_backfill)
    exit_code = cli.main(["backfill-running-style"])
    assert exit_code == 0
    assert seen_roots == [backfill_running_style.DEFAULT_ARTIFACT_ROOT]


def test_cmd_backfill_running_style_reports_errors(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def _fake_backfill(
        client: MlflowClient, artifact_root: Path, calibrator_dir: Path = Path()
    ) -> RunningStyleBackfillSummary:
        return RunningStyleBackfillSummary(
            versions_registered=0, errors=["bad file"], champion_sync={}
        )

    monkeypatch.setattr(backfill_running_style, "backfill_running_style", _fake_backfill)
    exit_code = cli.main(["backfill-running-style", "/nonexistent"])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "error: bad file" in captured.err


def test_cmd_ingest_trial_registry(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    duckdb_path = tmp_path / "trial_registry_jra.duckdb"
    con = duckdb.connect(str(duckdb_path))
    con.execute("CREATE TABLE trials (trial_id VARCHAR, model_version VARCHAR)")
    con.execute("INSERT INTO trials VALUES ('t1', 'v1')")
    con.close()

    exit_code = cli.main(["ingest-trial-registry", str(duckdb_path)])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "logged 1 run(s):" in captured.out


def test_cmd_ingest_serve_accuracy(
    tmp_path: Path, write_json: WriteJsonFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    json_path = tmp_path / "serve_accuracy.json"
    write_json(
        json_path,
        {"finish_position": {"date_str": "20260601", "category": "jra", "era": "POST_FIX"}},
    )
    exit_code = cli.main(["ingest-serve-accuracy", str(json_path), "--eval-regime", "serve"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "logged run:" in captured.out


def test_cmd_log_eval(
    tmp_path: Path, write_json: WriteJsonFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    json_path = tmp_path / "cell_report.json"
    write_json(json_path, [{"venue": "05", "top1": 0.4, "race_count": 10}])
    exit_code = cli.main(
        ["log-eval", str(json_path), "--eval-regime", "oos", "--run-name", "custom-run"]
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "logged run:" in captured.out


def test_cmd_ingest_local_pg_model_evaluations(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "model_prediction_evaluations.parquet"
    pd.DataFrame(
        [
            {
                "model_version": "jra-cb-v7-lineage",
                "category": "jra",
                "evaluation_window_from": "20240101",
                "evaluation_window_to": "20251231",
                "top1_accuracy": 0.3,
            }
        ]
    ).to_parquet(path)
    exit_code = cli.main(["ingest-local-pg-model-evaluations", str(path)])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "logged 1 run(s):" in captured.out


def test_cmd_ingest_local_pg_running_style_buckets(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "running_style_model_bucket_evaluations.parquet"
    pd.DataFrame(
        [
            {
                "model_version": "jra-running-style-lgbm-prod-v3",
                "category": "jra",
                "running_style_feature_version": "v1",
                "keibajo_code": "05",
                "race_count": 10,
                "cm_actual_nige_pred_nige_count": 10,
            }
        ]
    ).to_parquet(path)
    exit_code = cli.main(
        ["ingest-local-pg-running-style-buckets", str(path), "--eval-regime", "wf"]
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "logged 1 run(s):" in captured.out


def test_cmd_log_training_run(
    tmp_path: Path, write_json: WriteJsonFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest_path = tmp_path / "manifest.json"
    write_json(
        manifest_path,
        {
            "schema": "hr-mlflow-training-run/v1",
            "task": "finish-position",
            "category": "jra",
            "model_version": "jra-cb-cli-test-v1",
            "eval_regime": "wf",
        },
    )
    exit_code = cli.main(["log-training-run", str(manifest_path)])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "logged run:" in captured.out


def test_cmd_log_training_run_reports_error_on_malformed_manifest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{not valid json", encoding="utf-8")
    exit_code = cli.main(["log-training-run", str(manifest_path)])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "error:" in captured.err


def test_cmd_export_active_models(capsys: pytest.CaptureFixture[str]) -> None:
    # Fresh registry with no champions registered anywhere -- export-active-models
    # now fails loudly by default (Defect 2 fix), so --allow-missing is required
    # to exercise the plain success path here.
    exit_code = cli.main(["export-active-models", "--allow-missing"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "exported active models to:" in captured.out


def test_cmd_export_active_models_fails_when_champion_missing_without_flag(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = cli.main(["export-active-models"])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "error:" in captured.err
    assert "missing champion alias" in captured.err


def test_cmd_export_active_models_reports_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def _raise(
        client: MlflowClient,
        output_path: Path | None,
        *,
        upload_r2: str | None,
        allow_missing: bool = False,
    ) -> Path:
        raise ValueError("boom")

    monkeypatch.setattr(export_production, "export_active_models", _raise)
    exit_code = cli.main(["export-active-models"])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "error: boom" in captured.err


def test_cmd_export_cell_routing(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    client = MlflowClient(tracking_uri=_isolated_tracking_uri(tmp_path))
    version = registry.register_version(
        client, "jra-finish-position", f"file://{config.get_data_dir()}"
    )
    registry.set_champion(client, "jra-finish-position", version.version)
    exit_code = cli.main(["export-cell-routing", "--category", "jra"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "exported cell routing to:" in captured.out


def test_cmd_export_cell_routing_reports_error_when_no_champion(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = cli.main(["export-cell-routing", "--category", "jra"])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "error:" in captured.err


def test_cmd_set_champion(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    client = MlflowClient(tracking_uri=_isolated_tracking_uri(tmp_path))
    version = registry.register_version(
        client, "jra-finish-position", f"file://{config.get_data_dir()}"
    )
    exit_code = cli.main(["set-champion", "jra-finish-position", str(version.version)])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "champion alias set" in captured.out
    registered = client.get_registered_model("jra-finish-position")
    assert registry.CHAMPION_ALIAS in registered.aliases


def test_cmd_list_models_with_no_aliases(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    client = MlflowClient(tracking_uri=_isolated_tracking_uri(tmp_path))
    registry.register_version(client, "nar-finish-position", f"file://{config.get_data_dir()}")
    exit_code = cli.main(["list-models"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "nar-finish-position" in captured.out


def test_cmd_list_models_with_aliases(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    client = MlflowClient(tracking_uri=_isolated_tracking_uri(tmp_path))
    version = registry.register_version(
        client, "jra-finish-position", f"file://{config.get_data_dir()}"
    )
    registry.set_champion(client, "jra-finish-position", version.version)
    exit_code = cli.main(["list-models"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "champion=" in captured.out


def test_main_calls_load_dotenv_local_before_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(config, "load_dotenv_local", lambda: calls.append("loaded"))
    exit_code = cli.main(["list-models"])
    assert exit_code == 0
    assert calls == ["loaded"]


def test_main_calls_load_repo_root_env_fallback_after_dotenv_local(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(config, "load_dotenv_local", lambda: calls.append("dotenv_local"))
    monkeypatch.setattr(
        config, "load_repo_root_env_fallback", lambda: calls.append("root_env_fallback")
    )
    exit_code = cli.main(["list-models"])
    assert exit_code == 0
    assert calls == ["dotenv_local", "root_env_fallback"]


def test_main_requires_a_subcommand() -> None:
    with pytest.raises(SystemExit):
        cli.main([])


def test_main_rejects_unknown_subcommand() -> None:
    with pytest.raises(SystemExit):
        cli.main(["not-a-real-subcommand"])
