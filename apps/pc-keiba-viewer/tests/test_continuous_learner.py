"""Tests for continuous_learner module."""

from __future__ import annotations

import json
import signal as sig_mod
from collections.abc import Callable
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import pytest

import pandas as pd

import continuous_learner as subject
from feature_registry import FeatureEntry, FeatureRegistry


def _make_df() -> pd.DataFrame:
    rows = []
    for year in [2023, 2024]:
        for race in range(3):
            for horse in range(4):
                rows.append(
                    {
                        "source": "jra",
                        "race_date": f"{year}0601",
                        "kaisai_nen": str(year),
                        "kaisai_tsukihi": "0601",
                        "keibajo_code": "10",
                        "race_bango": f"{race:02d}",
                        "ketto_toroku_bango": f"horse_{horse:03d}",
                        "umaban": horse + 1,
                        "category": "jra",
                        "race_id": f"{year}_race_{race:02d}",
                        "race_year": year,
                        "feature_schema_version": "1",
                        "finish_position": horse + 1,
                        "finish_norm": 0.5,
                        "target_corner_1_norm": 0.5,
                        "target_corner_3_norm": 0.5,
                        "target_corner_4_norm": 0.5,
                        "target_running_style_class": 0,
                        "feat_speed": float(horse),
                        "feat_jockey": 0.3,
                    }
                )
    return pd.DataFrame(rows)


def _make_entry(
    ndcg: float = 0.60, feature_names: list[str] | None = None
) -> FeatureEntry:
    return FeatureEntry(
        id=1,
        trial_id="trial-x",
        ndcg_at_3=ndcg,
        is_active=True,
        feature_names=feature_names if feature_names is not None else ["feat_speed"],
        definition_json="{}",
        created_at="2026-01-01T00:00:00+00:00",
    )


def _make_learner(
    registry: FeatureRegistry | None = None,
    df: pd.DataFrame | None = None,
    category: str = "jra",
    repo_root: Path | None = None,
    scripts_dir: Path | None = None,
    docker_image_tag: str = subject.DEFAULT_DOCKER_TAG,
    n_trials_per_round: int = subject.DEFAULT_N_TRIALS,
    validation_years: list[int] | None = None,
    train_start: str = "20160101",
    deploy_threshold: float = subject.DEFAULT_DEPLOY_THRESHOLD,
    load_controller: subject.AdaptiveLoadController | None = None,
) -> subject.ContinuousLearner:
    return subject.ContinuousLearner(
        registry=registry
        if registry is not None
        else FeatureRegistry(Path(":memory:")),
        df=df if df is not None else _make_df(),
        category=category,
        repo_root=repo_root if repo_root is not None else Path("/fake/repo"),
        scripts_dir=scripts_dir if scripts_dir is not None else Path("/fake/scripts"),
        docker_image_tag=docker_image_tag,
        n_trials_per_round=n_trials_per_round,
        validation_years=validation_years,
        train_start=train_start,
        deploy_threshold=deploy_threshold,
        load_controller=load_controller,
    )


# ---------------------------------------------------------------------------
# write_filtered_parquet
# ---------------------------------------------------------------------------


def test_write_filtered_parquet_keeps_feature_and_meta_cols(tmp_path: Path) -> None:
    df = _make_df()
    out = subject.write_filtered_parquet(df, ["feat_speed"], tmp_path / "out")
    result = pd.read_parquet(out)
    assert "feat_speed" in result.columns
    assert "race_id" in result.columns
    assert "finish_position" in result.columns


def test_write_filtered_parquet_excludes_non_selected_features(tmp_path: Path) -> None:
    df = _make_df()
    out = subject.write_filtered_parquet(df, ["feat_speed"], tmp_path / "out")
    result = pd.read_parquet(out)
    assert "feat_jockey" not in result.columns


def test_write_filtered_parquet_creates_directory_if_missing(tmp_path: Path) -> None:
    df = _make_df()
    output_dir = tmp_path / "nested" / "dir"
    assert not output_dir.exists()
    subject.write_filtered_parquet(df, ["feat_speed"], output_dir)
    assert output_dir.exists()


def test_write_filtered_parquet_returns_path_to_parquet(tmp_path: Path) -> None:
    df = _make_df()
    out = subject.write_filtered_parquet(df, ["feat_speed"], tmp_path / "out")
    assert out.name == "features.parquet"
    assert out.exists()


# ---------------------------------------------------------------------------
# ContinuousLearner defaults
# ---------------------------------------------------------------------------


def test_learner_default_validation_years_matches_feature_explorer() -> None:
    from feature_explorer import DEFAULT_VALIDATION_YEARS

    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        assert learner._validation_years == DEFAULT_VALIDATION_YEARS


def test_learner_custom_validation_years_stored() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, validation_years=[2022, 2023])
        assert learner._validation_years == [2022, 2023]


def test_learner_initial_stop_flag_is_false() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        assert learner._stop is False


# ---------------------------------------------------------------------------
# request_stop
# ---------------------------------------------------------------------------


def test_request_stop_sets_stop_flag() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        learner.request_stop()
        assert learner._stop is True


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def test_run_with_max_rounds_zero_does_not_call_explore() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with patch.object(learner, "_explore_round") as mock_explore:
            learner.run(max_rounds=0)
            mock_explore.assert_not_called()


def test_run_with_max_rounds_two_calls_explore_twice() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with (
            patch.object(learner, "_explore_round") as mock_explore,
            patch.object(learner, "_maybe_deploy"),
        ):
            learner.run(max_rounds=2)
            assert mock_explore.call_count == 2


def test_run_does_not_iterate_if_already_stopped() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        learner.request_stop()
        with patch.object(learner, "_explore_round") as mock_explore:
            learner.run(max_rounds=None)
            mock_explore.assert_not_called()


def test_run_stops_when_stop_flag_set_mid_loop() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        call_count = 0

        def _fake_explore(round_num: int, n_trials: int | None = None) -> None:
            nonlocal call_count
            call_count += 1
            learner.request_stop()

        with (
            patch.object(learner, "_explore_round", side_effect=_fake_explore),
            patch.object(learner, "_maybe_deploy"),
        ):
            learner.run(max_rounds=None)

        assert call_count == 1


def test_run_calls_maybe_deploy_after_each_explore() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        order: list[str] = []

        with (
            patch.object(
                learner,
                "_explore_round",
                side_effect=lambda *a, **kw: order.append("explore"),
            ),
            patch.object(
                learner, "_maybe_deploy", side_effect=lambda: order.append("deploy")
            ),
        ):
            learner.run(max_rounds=2)

        assert order == ["explore", "deploy", "explore", "deploy"]


# ---------------------------------------------------------------------------
# _explore_round
# ---------------------------------------------------------------------------


def test_explore_round_calls_run_exploration_with_registry_and_df() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        df = _make_df()
        learner = _make_learner(registry=reg, df=df, validation_years=[2024])
        with patch("continuous_learner.run_exploration") as mock_run:
            learner._explore_round(0)
            mock_run.assert_called_once()
            kwargs = mock_run.call_args.kwargs
            assert kwargs["registry"] is reg
            assert kwargs["df"] is df
            assert kwargs["validation_years"] == [2024]


def test_explore_round_study_name_includes_category_and_round() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="nar")
        with patch("continuous_learner.run_exploration") as mock_run:
            learner._explore_round(3)
            study_name = mock_run.call_args.kwargs["study_name"]
            assert study_name.startswith("auto-nar-r3-")


def test_explore_round_uses_override_n_trials() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, n_trials_per_round=20)
        with patch("continuous_learner.run_exploration") as mock_run:
            learner._explore_round(0, n_trials=3)
            kwargs = mock_run.call_args.kwargs
            assert kwargs["n_trials"] == 3


# ---------------------------------------------------------------------------
# _maybe_deploy
# ---------------------------------------------------------------------------


def test_maybe_deploy_does_nothing_when_no_active_entry() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with patch.object(learner, "_deploy") as mock_deploy:
            learner._maybe_deploy()
            mock_deploy.assert_not_called()


def test_maybe_deploy_does_not_deploy_when_below_threshold() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("t1", 0.50, ["feat_speed"])
        reg.activate(1)
        reg.record_deployment(0.497, 1)
        # active = 0.50, deployed = 0.497, delta = 0.003 < threshold 0.005 → skip
        learner = _make_learner(registry=reg, deploy_threshold=0.005)
        with patch.object(learner, "_deploy") as mock_deploy:
            learner._maybe_deploy()
            mock_deploy.assert_not_called()


def test_maybe_deploy_deploys_when_above_threshold() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("t1", 0.80, ["feat_speed"])
        reg.activate(1)
        # deployed_ndcg = 0.0, active = 0.80, threshold = 0.005 → 0.80 > 0.005 → deploy
        learner = _make_learner(registry=reg, deploy_threshold=0.005)
        with patch.object(learner, "_deploy") as mock_deploy:
            learner._maybe_deploy()
            mock_deploy.assert_called_once()
            entry = mock_deploy.call_args.args[0]
            assert entry["trial_id"] == "t1"


def test_maybe_deploy_deploys_when_delta_equals_threshold() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("t1", 0.505, ["feat_speed"])
        reg.activate(1)
        reg.record_deployment(0.50, 1)
        # delta = 0.005 == threshold → deploys (strict < comparison)
        learner = _make_learner(registry=reg, deploy_threshold=0.005)
        with patch.object(learner, "_deploy") as mock_deploy:
            learner._maybe_deploy()
            mock_deploy.assert_called_once()


# ---------------------------------------------------------------------------
# _deploy
# ---------------------------------------------------------------------------


def test_deploy_calls_pipeline_steps_in_correct_order() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        entry = _make_entry(ndcg=0.75, feature_names=["feat_speed"])
        order: list[str] = []

        with (
            patch(
                "continuous_learner.write_filtered_parquet",
                side_effect=lambda *a, **kw: (
                    order.append("write"),
                    Path("/tmp/f.parquet"),
                )[1],
            ),
            patch.object(
                learner,
                "_train_production_model",
                side_effect=lambda *a, **kw: (
                    order.append("train"),
                    Path("/tmp/model"),
                )[1],
            ),
            patch.object(
                learner,
                "_stage_model",
                side_effect=lambda *a, **kw: order.append("stage"),
            ),
            patch.object(
                learner,
                "_update_model_meta_json",
                side_effect=lambda *a, **kw: order.append("meta"),
            ),
            patch.object(
                learner, "_rebuild_docker", side_effect=lambda: order.append("docker")
            ),
        ):
            learner._deploy(entry)

        assert order == ["write", "train", "stage", "meta", "docker"]


def test_deploy_records_deployment_in_registry() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        entry = _make_entry(ndcg=0.77, feature_names=["feat_a", "feat_b"])

        with (
            patch(
                "continuous_learner.write_filtered_parquet",
                return_value=Path("/tmp/f.parquet"),
            ),
            patch.object(
                learner, "_train_production_model", return_value=Path("/tmp/model")
            ),
            patch.object(learner, "_stage_model"),
            patch.object(learner, "_update_model_meta_json"),
            patch.object(learner, "_rebuild_docker"),
        ):
            learner._deploy(entry)

        assert reg.get_deployed_ndcg() == 0.77


# ---------------------------------------------------------------------------
# _make_model_version
# ---------------------------------------------------------------------------


def test_make_model_version_includes_category() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="ban-ei")
        version = learner._make_model_version()
        assert version.startswith("auto-ban-ei-")


def test_make_model_version_includes_timestamp_digits() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        version = learner._make_model_version()
        ts_part = version.replace("auto-jra-", "")
        assert ts_part.isdigit()
        assert len(ts_part) == 14


# ---------------------------------------------------------------------------
# _train_production_model
# ---------------------------------------------------------------------------


def test_train_production_model_uses_catboost_for_jra(tmp_path: Path) -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="jra", validation_years=[2024])
        with patch("subprocess.run") as mock_run:
            learner._train_production_model(
                Path("/p/features.parquet"), tmp_path / "models", "auto-jra-v1"
            )
        cmd = mock_run.call_args.args[0]
        assert "train_finish_position_catboost_walk_forward.py" in cmd[1]
        assert "--category" in cmd
        assert "jra" in cmd


def test_train_production_model_uses_xgboost_for_nar(tmp_path: Path) -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="nar", validation_years=[2024])
        with patch("subprocess.run") as mock_run:
            learner._train_production_model(
                Path("/p/features.parquet"), tmp_path / "models", "auto-nar-v1"
            )
        cmd = mock_run.call_args.args[0]
        assert "train_finish_position_xgboost_walk_forward.py" in cmd[1]


def test_train_production_model_uses_catboost_for_banei(tmp_path: Path) -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg, category="ban-ei", validation_years=[2024]
        )
        with patch("subprocess.run") as mock_run:
            learner._train_production_model(
                Path("/p/features.parquet"), tmp_path / "models", "auto-banei-v1"
            )
        cmd = mock_run.call_args.args[0]
        assert "train_finish_position_catboost_walk_forward.py" in cmd[1]


def test_train_production_model_defaults_to_catboost_for_unknown_category(
    tmp_path: Path,
) -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg, category="unknown-cat", validation_years=[2024]
        )
        with patch("subprocess.run") as mock_run:
            learner._train_production_model(
                Path("/p/features.parquet"), tmp_path / "models", "auto-v1"
            )
        cmd = mock_run.call_args.args[0]
        assert "train_finish_position_catboost_walk_forward.py" in cmd[1]


def test_train_production_model_returns_path_to_final_fold(tmp_path: Path) -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="jra", validation_years=[2024])
        with patch("subprocess.run"):
            result = learner._train_production_model(
                Path("/p/features.parquet"), tmp_path / "models", "auto-jra-v1"
            )
        expected = tmp_path / "models" / "jra" / "iter0" / "fold-2024"
        assert result == expected


def test_train_production_model_passes_train_start_date(tmp_path: Path) -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg,
            category="jra",
            validation_years=[2024],
            train_start="20130101",
        )
        with patch("subprocess.run") as mock_run:
            learner._train_production_model(
                Path("/p/features.parquet"), tmp_path / "models", "auto-jra-v1"
            )
        cmd = mock_run.call_args.args[0]
        assert "--train-start-date" in cmd
        idx = cmd.index("--train-start-date")
        assert cmd[idx + 1] == "20130101"


def test_train_production_model_uses_max_validation_year_for_fold(
    tmp_path: Path,
) -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg, category="jra", validation_years=[2022, 2024]
        )
        with patch("subprocess.run") as mock_run:
            result = learner._train_production_model(
                Path("/p/features.parquet"), tmp_path / "models", "auto-jra-v1"
            )
        cmd = mock_run.call_args.args[0]
        assert "--year-to" in cmd
        idx = cmd.index("--year-to")
        assert cmd[idx + 1] == "2024"
        assert result == tmp_path / "models" / "jra" / "iter0" / "fold-2024"


# ---------------------------------------------------------------------------
# _stage_model
# ---------------------------------------------------------------------------


def test_stage_model_copies_model_json(tmp_path: Path) -> None:
    model_dir = tmp_path / "model_dir"
    model_dir.mkdir()
    (model_dir / "model.json").write_text('{"booster": "catboost"}', encoding="utf-8")

    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="jra", repo_root=tmp_path)
        learner._stage_model(model_dir, ["feat_speed"], "auto-jra-20260101000000")

    dest = tmp_path / subject._CONTAINER_MODELS_ROOT / "jra" / "auto-jra-20260101000000"
    assert (dest / "model.json").exists()
    assert (dest / "model.json").read_text(
        encoding="utf-8"
    ) == '{"booster": "catboost"}'


def test_stage_model_writes_metadata_json_with_feature_names(tmp_path: Path) -> None:
    model_dir = tmp_path / "model_dir"
    model_dir.mkdir()
    (model_dir / "model.json").write_text("{}", encoding="utf-8")

    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="jra", repo_root=tmp_path)
        learner._stage_model(
            model_dir, ["feat_speed", "feat_jockey"], "auto-jra-20260101000000"
        )

    dest = tmp_path / subject._CONTAINER_MODELS_ROOT / "jra" / "auto-jra-20260101000000"
    metadata = json.loads((dest / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["feature_names"] == ["feat_speed", "feat_jockey"]


def test_stage_model_creates_destination_directory(tmp_path: Path) -> None:
    model_dir = tmp_path / "model_dir"
    model_dir.mkdir()
    (model_dir / "model.json").write_text("{}", encoding="utf-8")

    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="nar", repo_root=tmp_path)
        learner._stage_model(model_dir, ["feat_speed"], "auto-nar-v1")

    dest = tmp_path / subject._CONTAINER_MODELS_ROOT / "nar" / "auto-nar-v1"
    assert dest.is_dir()


# ---------------------------------------------------------------------------
# _update_model_meta_json
# ---------------------------------------------------------------------------


def test_update_model_meta_json_raises_when_file_missing(tmp_path: Path) -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="jra", repo_root=tmp_path)
        with pytest.raises(FileNotFoundError):
            learner._update_model_meta_json("auto-jra-v1", 150)


def test_update_model_meta_json_writes_updated_file(tmp_path: Path) -> None:
    json_path = tmp_path / subject._MODEL_META_JSON_PATH
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps({"model_versions": {}, "feature_counts": {}}),
        encoding="utf-8",
    )
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="jra", repo_root=tmp_path)
        learner._update_model_meta_json("auto-jra-v1", 150)
    data = json.loads(json_path.read_text(encoding="utf-8"))
    assert data["model_versions"]["jra"] == "auto-jra-v1"
    assert data["feature_counts"]["jra"] == 150


def test_update_model_meta_json_merges_with_existing_file(tmp_path: Path) -> None:
    json_path = tmp_path / subject._MODEL_META_JSON_PATH
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(
            {
                "model_versions": {"nar": "auto-nar-v0"},
                "feature_counts": {"nar": 120},
            }
        ),
        encoding="utf-8",
    )
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="jra", repo_root=tmp_path)
        learner._update_model_meta_json("auto-jra-v1", 150)
    data = json.loads(json_path.read_text(encoding="utf-8"))
    assert data["model_versions"]["jra"] == "auto-jra-v1"
    assert data["model_versions"]["nar"] == "auto-nar-v0"
    assert data["feature_counts"]["jra"] == 150
    assert data["feature_counts"]["nar"] == 120


def test_update_model_meta_json_updates_existing_category(tmp_path: Path) -> None:
    json_path = tmp_path / subject._MODEL_META_JSON_PATH
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(
            {
                "model_versions": {"jra": "auto-jra-old"},
                "feature_counts": {"jra": 100},
            }
        ),
        encoding="utf-8",
    )
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="jra", repo_root=tmp_path)
        learner._update_model_meta_json("auto-jra-new", 200)
    data = json.loads(json_path.read_text(encoding="utf-8"))
    assert data["model_versions"]["jra"] == "auto-jra-new"
    assert data["feature_counts"]["jra"] == 200


def test_update_model_meta_json_raises_for_non_dict_root_json(tmp_path: Path) -> None:
    json_path = tmp_path / subject._MODEL_META_JSON_PATH
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="jra", repo_root=tmp_path)
        with pytest.raises(ValueError):
            learner._update_model_meta_json("auto-jra-v1", 150)


def test_update_model_meta_json_handles_non_dict_model_versions(
    tmp_path: Path,
) -> None:
    json_path = tmp_path / subject._MODEL_META_JSON_PATH
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps({"model_versions": "not-a-dict", "feature_counts": {"nar": 100}}),
        encoding="utf-8",
    )
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="jra", repo_root=tmp_path)
        learner._update_model_meta_json("auto-jra-v1", 150)
    data = json.loads(json_path.read_text(encoding="utf-8"))
    assert data["model_versions"]["jra"] == "auto-jra-v1"
    assert "nar" not in data["model_versions"]
    assert data["feature_counts"]["nar"] == 100


def test_update_model_meta_json_handles_non_dict_feature_counts(
    tmp_path: Path,
) -> None:
    json_path = tmp_path / subject._MODEL_META_JSON_PATH
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps({"model_versions": {"nar": "auto-nar-v0"}, "feature_counts": "bad"}),
        encoding="utf-8",
    )
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="jra", repo_root=tmp_path)
        learner._update_model_meta_json("auto-jra-v1", 150)
    data = json.loads(json_path.read_text(encoding="utf-8"))
    assert data["model_versions"]["nar"] == "auto-nar-v0"
    assert "nar" not in data["feature_counts"]
    assert data["feature_counts"]["jra"] == 150


# ---------------------------------------------------------------------------
# _rebuild_docker
# ---------------------------------------------------------------------------


def test_rebuild_docker_calls_docker_build_with_tag() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg,
            repo_root=Path("/repo"),
            docker_image_tag="my-image:latest",
        )
        with patch("subprocess.run") as mock_run:
            learner._rebuild_docker()
        cmd = mock_run.call_args.args[0]
        assert "docker" in cmd
        assert "build" in cmd
        assert "my-image:latest" in cmd


def test_rebuild_docker_passes_dockerfile_and_build_context() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, repo_root=Path("/repo"))
        with patch("subprocess.run") as mock_run:
            learner._rebuild_docker()
        cmd = mock_run.call_args.args[0]
        assert "-f" in cmd
        df_idx = cmd.index("-f")
        assert "Dockerfile" in cmd[df_idx + 1]
        assert "/repo" in cmd


# ---------------------------------------------------------------------------
# setup_logging
# ---------------------------------------------------------------------------


def test_setup_logging_adds_stdout_handler_when_no_handlers_exist() -> None:
    import logging

    root = logging.getLogger()
    original_handlers = root.handlers[:]
    root.handlers.clear()
    try:
        subject.setup_logging()
        assert len(root.handlers) == 1
        import sys
        assert root.handlers[0].stream is sys.stdout  # type: ignore[attr-defined]
        assert root.level == logging.INFO
    finally:
        root.handlers.clear()
        root.handlers.extend(original_handlers)


def test_setup_logging_is_idempotent_when_handlers_already_exist() -> None:
    import logging

    root = logging.getLogger()
    original_handlers = root.handlers[:]
    sentinel = logging.StreamHandler()
    root.handlers.clear()
    root.handlers.append(sentinel)
    try:
        subject.setup_logging()
        assert len(root.handlers) == 1
        assert root.handlers[0] is sentinel
    finally:
        root.handlers.clear()
        root.handlers.extend(original_handlers)


# _setup_signal_handler
# ---------------------------------------------------------------------------


def test_setup_signal_handler_registers_sigint_and_sigterm() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with patch("signal.signal") as mock_signal:
            subject._setup_signal_handler(learner)
            registered = [c.args[0] for c in mock_signal.call_args_list]
            assert sig_mod.SIGINT in registered
            assert sig_mod.SIGTERM in registered


def test_setup_signal_handler_calls_request_stop_when_triggered() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        captured: dict[int, object] = {}

        def _capture(signum: int, handler: object) -> None:
            captured[signum] = handler

        with patch("signal.signal", side_effect=_capture):
            subject._setup_signal_handler(learner)

        assert learner._stop is False
        handler_fn = cast(Callable[[int, object], None], captured[sig_mod.SIGINT])
        handler_fn(sig_mod.SIGINT, None)
        assert learner._stop is True


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def test_main_runs_and_stops_after_max_rounds(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().to_parquet(parquet_path, index=False)
    registry_path = tmp_path / "reg.duckdb"

    explore_calls: list[int] = []

    def fake_explore(round_num: int, n_trials: int | None = None) -> None:
        explore_calls.append(round_num)

    with (
        patch.object(
            subject.ContinuousLearner, "_explore_round", side_effect=fake_explore
        ),
        patch.object(subject.ContinuousLearner, "_maybe_deploy"),
        patch.object(
            subject.AdaptiveLoadController,
            "_cpu_percent",
            return_value=60.0,
        ),
        patch.object(
            subject.AdaptiveLoadController,
            "_mem_percent",
            return_value=70.0,
        ),
    ):
        subject.main(
            [
                "--features-parquet",
                str(parquet_path),
                "--category",
                "jra",
                "--repo-root",
                str(tmp_path),
                "--registry-path",
                str(registry_path),
                "--max-rounds",
                "2",
            ]
        )

    assert len(explore_calls) == 2


def test_main_default_constants() -> None:
    assert subject.DEFAULT_DOCKER_TAG == "finish-position-predict-local:split2"
    assert subject.DEFAULT_DEPLOY_THRESHOLD == 0.005
    assert subject.DEFAULT_N_TRIALS == 20


def test_main_passes_pg_dsn_to_controller(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().to_parquet(parquet_path, index=False)
    registry_path = tmp_path / "reg.duckdb"

    created_controllers: list[subject.AdaptiveLoadController] = []
    original_init = subject.AdaptiveLoadController.__init__

    def capturing_init(
        self: subject.AdaptiveLoadController, *args: object, **kwargs: object
    ) -> None:
        original_init(self, *args, **kwargs)
        created_controllers.append(self)

    with (
        patch.object(subject.AdaptiveLoadController, "__init__", capturing_init),
        patch.object(subject.ContinuousLearner, "_explore_round"),
        patch.object(subject.ContinuousLearner, "_maybe_deploy"),
        patch.object(subject.AdaptiveLoadController, "_cpu_percent", return_value=60.0),
        patch.object(subject.AdaptiveLoadController, "_mem_percent", return_value=70.0),
    ):
        subject.main(
            [
                "--features-parquet",
                str(parquet_path),
                "--category",
                "jra",
                "--repo-root",
                str(tmp_path),
                "--registry-path",
                str(registry_path),
                "--max-rounds",
                "1",
                "--pg-dsn",
                "postgresql://user:pass@localhost/db",
            ]
        )

    assert len(created_controllers) == 1
    assert created_controllers[0]._pg_dsn == "postgresql://user:pass@localhost/db"


# ---------------------------------------------------------------------------
# AdaptiveLoadController
# ---------------------------------------------------------------------------


def test_adaptive_controller_returns_base_when_load_is_moderate() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=20)
    with (
        patch.object(ctrl, "_cpu_percent", return_value=60.0),
        patch.object(ctrl, "_mem_percent", return_value=70.0),
    ):
        result = ctrl.adjusted_n_trials()
    assert result == 20


def test_adaptive_controller_reduces_trials_when_cpu_high() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=20, min_n_trials=5)
    with (
        patch.object(ctrl, "_cpu_percent", return_value=85.0),
        patch.object(ctrl, "_mem_percent", return_value=40.0),
    ):
        result = ctrl.adjusted_n_trials()
    assert result == max(round(20 * 0.5), 5)


def test_adaptive_controller_reduces_trials_when_mem_high() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=20, min_n_trials=5)
    with (
        patch.object(ctrl, "_cpu_percent", return_value=40.0),
        patch.object(ctrl, "_mem_percent", return_value=85.0),
    ):
        result = ctrl.adjusted_n_trials()
    assert result == max(round(20 * 0.5), 5)


def test_adaptive_controller_increases_trials_when_both_low() -> None:
    ctrl = subject.AdaptiveLoadController(
        base_n_trials=20, max_n_trials=50, cpu_low_pct=50.0, mem_low_pct=60.0
    )
    with (
        patch.object(ctrl, "_cpu_percent", return_value=40.0),
        patch.object(ctrl, "_mem_percent", return_value=50.0),
    ):
        result = ctrl.adjusted_n_trials()
    assert result == min(round(20 * 1.25), 50)


def test_adaptive_controller_clamps_to_min() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=6, min_n_trials=5)
    with (
        patch.object(ctrl, "_cpu_percent", return_value=85.0),
        patch.object(ctrl, "_mem_percent", return_value=40.0),
    ):
        result = ctrl.adjusted_n_trials()
    assert result >= 5


def test_adaptive_controller_clamps_to_max() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=40, max_n_trials=50)
    with (
        patch.object(ctrl, "_cpu_percent", return_value=30.0),
        patch.object(ctrl, "_mem_percent", return_value=30.0),
    ):
        result = ctrl.adjusted_n_trials()
    assert result <= 50


def test_adaptive_controller_sleep_seconds_zero_when_normal() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=20)
    with (
        patch.object(ctrl, "_cpu_percent", return_value=60.0),
        patch.object(ctrl, "_mem_percent", return_value=70.0),
    ):
        result = ctrl.inter_round_sleep_seconds()
    assert result == 0.0


def test_adaptive_controller_sleep_seconds_nonzero_when_high() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=20)
    with (
        patch.object(ctrl, "_cpu_percent", return_value=85.0),
        patch.object(ctrl, "_mem_percent", return_value=40.0),
    ):
        result = ctrl.inter_round_sleep_seconds()
    assert result == 5.0


def test_adaptive_controller_pg_active_count_returns_none_when_no_dsn() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=20, pg_dsn=None)
    result = ctrl._pg_active_count()
    assert result is None


def test_adaptive_controller_cpu_percent_returns_zero_without_psutil() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=20)
    with patch.object(subject, "_PSUTIL_AVAILABLE", False):
        result = ctrl._cpu_percent()
    assert result == 0.0


def test_adaptive_controller_cpu_percent_returns_float_when_psutil_available() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=20)
    with patch.object(subject, "_PSUTIL_AVAILABLE", True):
        result = ctrl._cpu_percent()
    assert isinstance(result, float)
    assert 0.0 <= result <= 100.0


def test_adaptive_controller_mem_percent_returns_zero_without_psutil() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=20)
    with patch.object(subject, "_PSUTIL_AVAILABLE", False):
        result = ctrl._mem_percent()
    assert result == 0.0


def test_adaptive_controller_mem_percent_returns_float_when_psutil_available() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=20)
    with patch.object(subject, "_PSUTIL_AVAILABLE", True):
        result = ctrl._mem_percent()
    assert isinstance(result, float)
    assert 0.0 <= result <= 100.0


def test_adaptive_controller_pg_active_count_returns_none_when_connection_fails() -> None:
    ctrl = subject.AdaptiveLoadController(
        base_n_trials=20, pg_dsn="postgresql://localhost:59999/nonexistent_db"
    )
    result = ctrl._pg_active_count()
    assert result is None


# ---------------------------------------------------------------------------
# ContinuousLearner with controller
# ---------------------------------------------------------------------------


def test_run_with_controller_calls_round_params() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        ctrl = MagicMock(spec=subject.AdaptiveLoadController)
        ctrl.round_params.return_value = (15, 0.0)
        learner = _make_learner(
            registry=reg, n_trials_per_round=20, load_controller=ctrl
        )
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy"),
        ):
            learner.run(max_rounds=1)
        ctrl.round_params.assert_called_once()


def test_run_with_controller_sleeps_when_nonzero() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        ctrl = MagicMock(spec=subject.AdaptiveLoadController)
        ctrl.round_params.return_value = (20, 5.0)
        learner = _make_learner(
            registry=reg, n_trials_per_round=20, load_controller=ctrl
        )
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy"),
            patch("continuous_learner.time.sleep") as mock_sleep,
        ):
            learner.run(max_rounds=1)
        mock_sleep.assert_called_once_with(5.0)


def test_run_without_controller_no_sleep() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, load_controller=None)
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy"),
            patch("continuous_learner.time.sleep") as mock_sleep,
        ):
            learner.run(max_rounds=2)
        mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# AdaptiveLoadController.round_params
# ---------------------------------------------------------------------------


def test_adaptive_controller_round_params_high_load_reduces_trials_and_returns_sleep() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=20, min_n_trials=5)
    with (
        patch.object(ctrl, "_cpu_percent", return_value=85.0),
        patch.object(ctrl, "_mem_percent", return_value=40.0),
    ):
        n_trials, sleep_secs = ctrl.round_params()
    assert n_trials == max(round(20 * 0.5), 5)
    assert sleep_secs == 5.0


def test_adaptive_controller_round_params_low_load_increases_trials_and_no_sleep() -> None:
    ctrl = subject.AdaptiveLoadController(
        base_n_trials=20, max_n_trials=50, cpu_low_pct=50.0, mem_low_pct=60.0
    )
    with (
        patch.object(ctrl, "_cpu_percent", return_value=40.0),
        patch.object(ctrl, "_mem_percent", return_value=50.0),
    ):
        n_trials, sleep_secs = ctrl.round_params()
    assert n_trials == min(round(20 * 1.25), 50)
    assert sleep_secs == 0.0


def test_adaptive_controller_round_params_moderate_load_returns_base_and_no_sleep() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=20)
    with (
        patch.object(ctrl, "_cpu_percent", return_value=60.0),
        patch.object(ctrl, "_mem_percent", return_value=70.0),
    ):
        n_trials, sleep_secs = ctrl.round_params()
    assert n_trials == 20
    assert sleep_secs == 0.0


def test_adaptive_controller_round_params_polls_cpu_mem_once() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=20)
    with (
        patch.object(ctrl, "_cpu_percent", return_value=60.0) as mock_cpu,
        patch.object(ctrl, "_mem_percent", return_value=70.0) as mock_mem,
    ):
        ctrl.round_params()
    assert mock_cpu.call_count == 1
    assert mock_mem.call_count == 1


# ---------------------------------------------------------------------------
# _stage_model returns Path / _update_model_meta_json returns prev content
# ---------------------------------------------------------------------------


def test_stage_model_returns_dest_path(tmp_path: Path) -> None:
    model_dir = tmp_path / "model_dir"
    model_dir.mkdir()
    (model_dir / "model.json").write_text("{}", encoding="utf-8")

    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="jra", repo_root=tmp_path)
        result = learner._stage_model(model_dir, ["feat_speed"], "auto-jra-20260101000000")

    expected = tmp_path / subject._CONTAINER_MODELS_ROOT / "jra" / "auto-jra-20260101000000"
    assert result == expected


def test_update_model_meta_json_returns_previous_content(tmp_path: Path) -> None:
    import json as _json

    json_path = tmp_path / subject._MODEL_META_JSON_PATH
    json_path.parent.mkdir(parents=True, exist_ok=True)
    original = _json.dumps({"model_versions": {"nar": "old-v"}, "feature_counts": {"nar": 50}})
    json_path.write_text(original, encoding="utf-8")

    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="jra", repo_root=tmp_path)
        prev = learner._update_model_meta_json("auto-jra-v1", 100)

    assert prev == original


# ---------------------------------------------------------------------------
# _rollback_deploy
# ---------------------------------------------------------------------------


def test_rollback_deploy_removes_staged_dir(tmp_path: Path) -> None:
    staged = tmp_path / "staged_model"
    staged.mkdir()
    (staged / "model.json").write_text("{}", encoding="utf-8")

    json_path = tmp_path / subject._MODEL_META_JSON_PATH
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text("{}", encoding="utf-8")

    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, repo_root=tmp_path)
        learner._rollback_deploy(staged, "{}")

    assert not staged.exists()


def test_rollback_deploy_restores_meta_json(tmp_path: Path) -> None:
    staged = tmp_path / "staged_model"
    staged.mkdir()

    json_path = tmp_path / subject._MODEL_META_JSON_PATH
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text('{"after": true}', encoding="utf-8")

    original_content = '{"before": true}'

    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, repo_root=tmp_path)
        learner._rollback_deploy(staged, original_content)

    assert json_path.read_text(encoding="utf-8") == original_content


def test_rollback_deploy_tolerates_missing_staged_dir(tmp_path: Path) -> None:
    staged = tmp_path / "nonexistent_dir"

    json_path = tmp_path / subject._MODEL_META_JSON_PATH
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text("{}", encoding="utf-8")

    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, repo_root=tmp_path)
        learner._rollback_deploy(staged, "{}")

    assert json_path.read_text(encoding="utf-8") == "{}"


def test_rollback_deploy_handles_rmtree_error(tmp_path: Path) -> None:
    staged = tmp_path / "staged_model"
    staged.mkdir()

    json_path = tmp_path / subject._MODEL_META_JSON_PATH
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text("{}", encoding="utf-8")

    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, repo_root=tmp_path)
        with patch("continuous_learner.shutil.rmtree", side_effect=OSError("cannot remove")):
            learner._rollback_deploy(staged, "{}")

    assert json_path.read_text(encoding="utf-8") == "{}"


def test_rollback_deploy_handles_write_error(tmp_path: Path) -> None:
    staged = tmp_path / "nonexistent_dir"

    json_path = tmp_path / subject._MODEL_META_JSON_PATH
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text('{"current": true}', encoding="utf-8")

    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, repo_root=tmp_path)
        with patch.object(Path, "write_text", side_effect=OSError("disk full")):
            learner._rollback_deploy(staged, '{"original": true}')


def test_deploy_rollback_when_docker_fails(tmp_path: Path) -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        entry = _make_entry(ndcg=0.75, feature_names=["feat_speed"])

        staged_path = tmp_path / "staged"

        with (
            patch(
                "continuous_learner.write_filtered_parquet",
                return_value=Path("/tmp/f.parquet"),
            ),
            patch.object(
                learner, "_train_production_model", return_value=Path("/tmp/model")
            ),
            patch.object(learner, "_stage_model", return_value=staged_path),
            patch.object(learner, "_update_model_meta_json", return_value='{"prev": true}'),
            patch.object(
                learner, "_rebuild_docker", side_effect=RuntimeError("docker failed")
            ),
            patch.object(learner, "_rollback_deploy") as mock_rollback,
        ):
            with pytest.raises(RuntimeError, match="docker failed"):
                learner._deploy(entry)

        mock_rollback.assert_called_once_with(staged_path, '{"prev": true}')


def test_deploy_does_not_rollback_on_success(tmp_path: Path) -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        entry = _make_entry(ndcg=0.75, feature_names=["feat_speed"])

        with (
            patch(
                "continuous_learner.write_filtered_parquet",
                return_value=Path("/tmp/f.parquet"),
            ),
            patch.object(
                learner, "_train_production_model", return_value=Path("/tmp/model")
            ),
            patch.object(learner, "_stage_model", return_value=Path("/tmp/staged")),
            patch.object(learner, "_update_model_meta_json", return_value='{"prev": true}'),
            patch.object(learner, "_rebuild_docker"),
            patch.object(learner, "_rollback_deploy") as mock_rollback,
        ):
            learner._deploy(entry)

        mock_rollback.assert_not_called()


def test_deploy_rollback_when_update_meta_fails(tmp_path: Path) -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        entry = _make_entry(ndcg=0.75, feature_names=["feat_speed"])
        staged_path = tmp_path / "staged"

        with (
            patch(
                "continuous_learner.write_filtered_parquet",
                return_value=Path("/tmp/f.parquet"),
            ),
            patch.object(
                learner, "_train_production_model", return_value=Path("/tmp/model")
            ),
            patch.object(learner, "_stage_model", return_value=staged_path),
            patch.object(
                learner,
                "_update_model_meta_json",
                side_effect=RuntimeError("write failed"),
            ),
            patch.object(learner, "_rollback_deploy") as mock_rollback,
        ):
            with pytest.raises(RuntimeError, match="write failed"):
                learner._deploy(entry)

        mock_rollback.assert_called_once_with(staged_path, None)


def test_rollback_deploy_skips_meta_restore_when_prev_content_is_none(tmp_path: Path) -> None:
    staged = tmp_path / "staged_model"
    staged.mkdir()

    json_path = tmp_path / subject._MODEL_META_JSON_PATH
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text('{"current": true}', encoding="utf-8")

    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, repo_root=tmp_path)
        learner._rollback_deploy(staged, None)

    assert not staged.exists()
    assert json_path.read_text(encoding="utf-8") == '{"current": true}'
