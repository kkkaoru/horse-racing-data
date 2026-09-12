"""Regression tests for service selection and cross-workspace build inputs."""

import sys

import pytest

from plan import main, select_targets


def test_documentation_does_not_redeploy_production() -> None:
    assert (
        select_targets(
            ["README.md", ".devin/wiki.json", ".github/workflows/production.yml"], "changed"
        )
        == []
    )


def test_deleted_or_renamed_inputs_are_selected_in_deploy_order() -> None:
    assert select_targets(
        ["apps/pc-keiba-viewer/src/old.ts", "apps/pc-keiba-r2-catalog/src/new.ts"], "changed"
    ) == ["pc-keiba-r2-catalog", "pc-keiba-viewer"]


def test_viewer_pipeline_also_rebuilds_prediction_container() -> None:
    assert select_targets(
        ["apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py"], "changed"
    ) == ["finish-position-cron", "pc-keiba-viewer"]


def test_hot_worker_is_a_viewer_build_input() -> None:
    assert select_targets(["apps/sync-realtime-data-hot/src/worker.ts"], "changed") == [
        "sync-realtime-data-hot",
        "pc-keiba-viewer",
    ]


def test_prediction_models_rebuild_cron_container() -> None:
    assert select_targets(
        ["apps/finish-position-predict-container/production-artifacts.json"], "changed"
    ) == ["finish-position-cron"]


def test_mlflow_python_is_a_proxy_container_input() -> None:
    assert select_targets(["apps/mlflow/src/client.py"], "changed") == ["mlflow-ui-proxy"]


def test_docker_helper_selects_all_container_hosts() -> None:
    assert select_targets(["scripts/ensure-docker-compat.sh"], "changed") == [
        "finish-position-cron",
        "mlflow-ui-proxy",
        "jra-van-datalab-cloudflare-demo",
    ]


@pytest.mark.parametrize(
    "path", ["bun.lock", "package.json", "packages/horse-racing-schema/src/race.ts"]
)
def test_shared_dependencies_select_every_service(path: str) -> None:
    assert select_targets([path], "changed") == [
        "pc-keiba-r2-catalog",
        "jra-van-datalab-worker-only-probe",
        "umacon-worker",
        "daily-keiba-sync",
        "sync-realtime-data-features",
        "sync-realtime-data-hot",
        "sync-realtime-data",
        "finish-position-cron",
        "pc-keiba-viewer",
        "pipeline-health-monitor",
        "venue-weather",
        "mlflow-ui-proxy",
        "jra-van-datalab-cloudflare-demo",
    ]


def test_explicit_target_ignores_diff() -> None:
    assert select_targets(["bun.lock"], "pipeline-health-monitor") == ["pipeline-health-monitor"]


def test_explicit_all_works_without_a_diff() -> None:
    assert len(select_targets([], "all")) == 13


def test_unknown_target_fails_closed() -> None:
    with pytest.raises(ValueError, match="Unknown deployment target"):
        select_targets([], "../../other")


def test_similarly_named_directory_does_not_match() -> None:
    assert select_targets(["apps/pc-keiba-viewer-backup/src/index.ts"], "changed") == []


def test_cli_reads_nul_delimited_paths(tmp_path, monkeypatch, capsys) -> None:
    paths = tmp_path / "paths"
    paths.write_text("apps/venue-weather/src/index.ts\0", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["plan.py", "--paths", str(paths), "--target", "changed"])
    main()
    assert capsys.readouterr().out == '["venue-weather"]\n'
