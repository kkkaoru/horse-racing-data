"""Tests for continuous_learner module."""

from __future__ import annotations

import json
import signal as sig_mod
from collections.abc import Callable
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import pytest

import polars as pl

import learning.continuous_learner as subject
from learning.feature_explorer import select_round_validation_years
from learning.feature_registry import FeatureEntry, FeatureRegistry
from finish_position_lightgbm import split_walk_forward


def _make_df() -> pl.DataFrame:
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
    return pl.DataFrame(rows)


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
    df: pl.DataFrame | None = None,
    category: str = "jra",
    repo_root: Path | None = None,
    scripts_dir: Path | None = None,
    docker_image_tag: str = subject.DEFAULT_DOCKER_TAG,
    n_trials_per_round: int = subject.DEFAULT_N_TRIALS,
    validation_years: list[int] | None = None,
    validation_year_pool: list[int] | None = None,
    blind_holdout_year: int | None = None,
    train_start: str = "20160101",
    deploy_threshold: float = subject.DEFAULT_DEPLOY_THRESHOLD,
    docker_build: bool = False,
    skip_inverse: bool = False,
    skip_enrichment: bool = False,
    load_controller: subject.AdaptiveLoadController | None = None,
    auto_tune: bool = True,
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
        validation_year_pool=validation_year_pool,
        blind_holdout_year=blind_holdout_year,
        train_start=train_start,
        deploy_threshold=deploy_threshold,
        docker_build=docker_build,
        skip_inverse=skip_inverse,
        skip_enrichment=skip_enrichment,
        load_controller=load_controller,
        auto_tune=auto_tune,
    )


# ---------------------------------------------------------------------------
# write_filtered_parquet
# ---------------------------------------------------------------------------


def test_write_filtered_parquet_keeps_feature_and_meta_cols(tmp_path: Path) -> None:
    df = _make_df()
    out = subject.write_filtered_parquet(df, ["feat_speed"], tmp_path / "out")
    result = pl.read_parquet(sorted(out.glob("race_year=*/*.parquet")))
    assert "feat_speed" in result.columns
    assert "race_id" in result.columns
    assert "finish_position" in result.columns


def test_write_filtered_parquet_excludes_non_selected_features(tmp_path: Path) -> None:
    df = _make_df()
    out = subject.write_filtered_parquet(df, ["feat_speed"], tmp_path / "out")
    result = pl.read_parquet(sorted(out.glob("race_year=*/*.parquet")))
    assert "feat_jockey" not in result.columns


def test_write_filtered_parquet_creates_directory_if_missing(tmp_path: Path) -> None:
    df = _make_df()
    output_dir = tmp_path / "nested" / "dir"
    assert not output_dir.exists()
    subject.write_filtered_parquet(df, ["feat_speed"], output_dir)
    assert output_dir.exists()


def test_write_filtered_parquet_returns_dataset_directory(tmp_path: Path) -> None:
    df = _make_df()
    out = subject.write_filtered_parquet(df, ["feat_speed"], tmp_path / "out")
    assert out == tmp_path / "out"
    assert out.is_dir()


def test_write_filtered_parquet_is_hive_partitioned_by_year(tmp_path: Path) -> None:
    df = _make_df()
    out = subject.write_filtered_parquet(df, ["feat_speed"], tmp_path / "out")
    partitions = sorted(p.name for p in out.glob("race_year=*") if p.is_dir())
    assert partitions == ["race_year=2023", "race_year=2024"]


def test_write_filtered_parquet_partition_holds_only_its_year(tmp_path: Path) -> None:
    df = _make_df()
    out = subject.write_filtered_parquet(df, ["feat_speed"], tmp_path / "out")
    part = pl.read_parquet(out / "race_year=2023" / "part-0.parquet")
    assert part["race_year"].unique().to_list() == [2023]
    assert part.height == 12


def test_write_filtered_parquet_keeps_race_year_when_not_a_feature(tmp_path: Path) -> None:
    df = _make_df()
    out = subject.write_filtered_parquet(df, ["feat_speed"], tmp_path / "out")
    result = pl.read_parquet(sorted(out.glob("race_year=*/*.parquet")))
    assert "race_year" in result.columns


# ---------------------------------------------------------------------------
# ContinuousLearner defaults
# ---------------------------------------------------------------------------


def test_learner_default_validation_years_matches_feature_explorer() -> None:
    from learning.feature_explorer import DEFAULT_VALIDATION_YEARS

    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        assert learner._validation_years == DEFAULT_VALIDATION_YEARS


def test_learner_custom_validation_years_stored() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, validation_years=[2022, 2023])
        assert learner._validation_years == [2022, 2023]


def test_learner_raises_when_validation_years_is_empty() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        with pytest.raises(ValueError, match="non-empty"):
            subject.ContinuousLearner(
                registry=reg,
                df=pl.DataFrame(),
                category="jra",
                repo_root=Path("/tmp"),
                scripts_dir=Path("/tmp"),
                validation_years=[],
            )


def test_learner_initial_stop_flag_is_false() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        assert learner._stop is False


def test_derive_blind_holdout_year_uses_max_year_from_df() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, df=_make_df())
        assert learner._blind_holdout_year == 2024


def test_blind_holdout_year_defaults_to_pool_max_for_empty_df() -> None:
    from learning.feature_explorer import VALIDATION_YEAR_POOL

    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg, df=pl.DataFrame(), validation_years=[2024]
        )
        assert learner._blind_holdout_year == max(VALIDATION_YEAR_POOL)


def test_blind_holdout_year_falls_back_when_no_race_year_column() -> None:
    from learning.feature_explorer import VALIDATION_YEAR_POOL

    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg, df=pl.DataFrame({"other_col": [1, 2, 3]})
        )
        assert learner._blind_holdout_year == max(VALIDATION_YEAR_POOL)


def test_blind_holdout_year_falls_back_when_race_year_all_nan() -> None:
    from learning.feature_explorer import VALIDATION_YEAR_POOL

    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg,
            df=pl.DataFrame({"race_year": [None, None, None]}),
        )
        assert learner._blind_holdout_year == max(VALIDATION_YEAR_POOL)


def test_blind_holdout_year_explicit_override_is_stored() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, blind_holdout_year=2022)
        assert learner._blind_holdout_year == 2022


def test_validation_year_pool_defaults_when_none() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        assert learner._validation_year_pool == [2021, 2022, 2023, 2024, 2025]


def test_validation_year_pool_custom_is_stored() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, validation_year_pool=[2020, 2021])
        assert learner._validation_year_pool == [2020, 2021]


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

        def _fake_explore(round_num: int, n_trials: int) -> None:
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


def test_learner_saturated_defaults_to_false() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        assert learner._saturated is False


def test_run_sets_saturated_from_maybe_deploy_return() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy", return_value=True),
        ):
            learner.run(max_rounds=1)
        assert learner._saturated is True


def test_run_halves_trials_after_saturation_detected() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, n_trials_per_round=20)
        trials_seen: list[int] = []

        def _record(round_num: int, n_trials: int) -> None:
            trials_seen.append(n_trials)

        with (
            patch.object(learner, "_explore_round", side_effect=_record),
            patch.object(learner, "_maybe_deploy", return_value=True),
        ):
            learner.run(max_rounds=2)

        assert trials_seen == [20, 10]


def test_run_does_not_halve_trials_when_not_saturated() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, n_trials_per_round=20)
        trials_seen: list[int] = []

        def _record(round_num: int, n_trials: int) -> None:
            trials_seen.append(n_trials)

        with (
            patch.object(learner, "_explore_round", side_effect=_record),
            patch.object(learner, "_maybe_deploy", return_value=False),
        ):
            learner.run(max_rounds=2)

        assert trials_seen == [20, 20]


def test_run_saturated_trials_floor_is_min_saturated_trials() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, n_trials_per_round=8)
        trials_seen: list[int] = []

        def _record(round_num: int, n_trials: int) -> None:
            trials_seen.append(n_trials)

        with (
            patch.object(learner, "_explore_round", side_effect=_record),
            patch.object(learner, "_maybe_deploy", return_value=True),
        ):
            learner.run(max_rounds=2)

        assert trials_seen == [8, subject._MIN_SATURATED_TRIALS]


def test_run_saturation_latches_and_does_not_reset_when_maybe_deploy_returns_false() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        returns = iter([True, False])
        with (
            patch.object(learner, "_explore_round"),
            patch.object(
                learner, "_maybe_deploy", side_effect=lambda: next(returns)
            ),
            patch.object(learner, "_check_and_try_inverses"),
            patch.object(learner, "_analyze_feature_enrichment"),
        ):
            learner.run(max_rounds=2)
        assert learner._saturated is True


def test_run_latched_saturation_halves_trials_in_later_round() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, n_trials_per_round=20)
        returns = iter([True, False])
        trials_seen: list[int] = []

        def _record(round_num: int, n_trials: int) -> None:
            trials_seen.append(n_trials)

        with (
            patch.object(learner, "_explore_round", side_effect=_record),
            patch.object(
                learner, "_maybe_deploy", side_effect=lambda: next(returns)
            ),
            patch.object(learner, "_check_and_try_inverses"),
            patch.object(learner, "_analyze_feature_enrichment"),
        ):
            learner.run(max_rounds=2)
        assert trials_seen == [20, 10]


def test_run_latched_saturation_skips_inverse_and_enrichment_in_later_round() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        returns = iter([True, False])
        with (
            patch.object(learner, "_explore_round"),
            patch.object(
                learner, "_maybe_deploy", side_effect=lambda: next(returns)
            ),
            patch.object(learner, "_check_and_try_inverses") as mock_check,
            patch.object(learner, "_analyze_feature_enrichment") as mock_enrich,
        ):
            learner.run(max_rounds=2)
        mock_check.assert_not_called()
        mock_enrich.assert_not_called()


def test_run_logs_saturation_latched_message_on_first_saturation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy", return_value=True),
            caplog.at_level("INFO", logger="learning.continuous_learner"),
        ):
            learner.run(max_rounds=1)
        latch_logs = [
            r.message for r in caplog.records if "saturation latched" in r.message
        ]
        assert len(latch_logs) == 1


def test_run_logs_saturation_latched_only_once_across_rounds(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy", return_value=True),
            patch.object(learner, "_check_and_try_inverses"),
            patch.object(learner, "_analyze_feature_enrichment"),
            caplog.at_level("INFO", logger="learning.continuous_learner"),
        ):
            learner.run(max_rounds=3)
        latch_logs = [
            r.message for r in caplog.records if "saturation latched" in r.message
        ]
        assert len(latch_logs) == 1


# ---------------------------------------------------------------------------
# _explore_round
# ---------------------------------------------------------------------------


def test_explore_round_calls_run_exploration_with_registry_and_df() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        df = _make_df()
        learner = _make_learner(
            registry=reg,
            df=df,
            validation_year_pool=[2021, 2022, 2023],
            blind_holdout_year=2023,
        )
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._explore_round(0, n_trials=20)
            mock_run.assert_called_once()
            kwargs = mock_run.call_args.kwargs
            assert kwargs["registry"] is reg
            assert kwargs["df"] is df
            years = kwargs["validation_years"]
            assert len(years) == 2
            assert 2023 not in years
            assert set(years).issubset({2021, 2022})
            assert years == sorted(years)


def test_explore_round_validation_years_vary_across_rounds() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg,
            validation_year_pool=[2021, 2022, 2023, 2024, 2025],
            blind_holdout_year=2025,
        )
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._explore_round(0, n_trials=20)
            years_round_0 = mock_run.call_args.kwargs["validation_years"]
            learner._explore_round(5, n_trials=20)
            years_round_5 = mock_run.call_args.kwargs["validation_years"]
        assert years_round_0 != years_round_5


def test_explore_round_study_name_includes_category_and_round() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="nar")
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._explore_round(3, n_trials=20)
            study_name = mock_run.call_args.kwargs["study_name"]
            assert study_name.startswith("auto-nar-r3-")


def test_explore_round_uses_override_n_trials() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, n_trials_per_round=20)
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._explore_round(0, n_trials=3)
            kwargs = mock_run.call_args.kwargs
            assert kwargs["n_trials"] == 3


def test_explore_round_passes_priority_subsets_and_timeout() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        active_id = reg.record_trial(
            "active", 0.8, ["feat_speed", "feat_jockey", "umaban", "race_id", "barei"], "{}"
        )
        reg.activate(active_id)
        learner = subject.ContinuousLearner(
            registry=reg,
            df=_make_df(),
            category="jra",
            repo_root=Path("/fake/repo"),
            scripts_dir=Path("/fake/scripts"),
            per_trial_timeout_s=45.0,
        )
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._explore_round(0, n_trials=20)
            kwargs = mock_run.call_args.kwargs
            assert kwargs["per_trial_timeout_s"] == pytest.approx(45.0)
            assert kwargs["enqueue_subsets"] == [
                {"feat_speed", "feat_jockey", "umaban", "race_id", "barei"}
            ]


def test_explore_round_passes_screening_true() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._explore_round(0, n_trials=20)
        assert mock_run.call_args.kwargs["screening"] is True


def test_explore_round_uses_two_validation_years_when_not_saturated() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg,
            validation_year_pool=[2021, 2022, 2023],
            blind_holdout_year=2023,
        )
        learner._saturated = False
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._explore_round(0, n_trials=20)
        assert len(mock_run.call_args.kwargs["validation_years"]) == 2


def test_explore_round_uses_single_validation_year_when_saturated() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg,
            validation_year_pool=[2021, 2022, 2023],
            blind_holdout_year=2023,
        )
        learner._saturated = True
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._explore_round(0, n_trials=20)
        assert len(mock_run.call_args.kwargs["validation_years"]) == 1


# ---------------------------------------------------------------------------
# _priority_subsets
# ---------------------------------------------------------------------------


def test_priority_subsets_empty_when_no_active_entry() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        assert learner._priority_subsets() == []


def test_priority_subsets_returns_active_set_only_when_no_enriched() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        active_id = reg.record_trial(
            "active", 0.8, ["feat_speed", "feat_jockey", "umaban", "race_id", "barei"], "{}"
        )
        reg.activate(active_id)
        learner = _make_learner(registry=reg)
        with patch.object(reg, "compute_feature_enrichment", return_value=[]):
            subsets = learner._priority_subsets()
    assert subsets == [{"feat_speed", "feat_jockey", "umaban", "race_id", "barei"}]


def test_priority_subsets_appends_active_plus_enriched_candidates() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        active_id = reg.record_trial(
            "active", 0.8, ["feat_speed", "feat_jockey", "umaban", "race_id", "barei"], "{}"
        )
        reg.activate(active_id)
        learner = _make_learner(registry=reg)
        with patch.object(
            reg,
            "compute_feature_enrichment",
            return_value=[("feat_new", 0.6), ("feat_speed", 0.5)],
        ):
            subsets = learner._priority_subsets()
    # feat_speed already active → filtered out; feat_new (positive score) added.
    assert subsets[0] == {"feat_speed", "feat_jockey", "umaban", "race_id", "barei"}
    assert subsets[1] == {"feat_speed", "feat_jockey", "umaban", "race_id", "barei", "feat_new"}


def test_priority_subsets_ignores_non_positive_enrichment_scores() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        active_id = reg.record_trial(
            "active", 0.8, ["feat_speed", "feat_jockey", "umaban", "race_id", "barei"], "{}"
        )
        reg.activate(active_id)
        learner = _make_learner(registry=reg)
        with patch.object(
            reg,
            "compute_feature_enrichment",
            return_value=[("feat_bad", -0.4), ("feat_zero", 0.0)],
        ):
            subsets = learner._priority_subsets()
    assert len(subsets) == 1


def test_priority_subsets_caches_enrichment_for_reuse() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        active_id = reg.record_trial(
            "active", 0.8, ["feat_speed", "feat_jockey", "umaban", "race_id", "barei"], "{}"
        )
        reg.activate(active_id)
        learner = _make_learner(registry=reg)
        with patch.object(
            reg, "compute_feature_enrichment", return_value=[("feat_new", 0.6)]
        ):
            learner._priority_subsets()
    assert learner._last_enrichment == [("feat_new", 0.6)]


def test_priority_subsets_caches_empty_enrichment() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        active_id = reg.record_trial(
            "active", 0.8, ["feat_speed", "feat_jockey", "umaban", "race_id", "barei"], "{}"
        )
        reg.activate(active_id)
        learner = _make_learner(registry=reg)
        with patch.object(reg, "compute_feature_enrichment", return_value=[]):
            learner._priority_subsets()
    assert learner._last_enrichment == []


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
        with (
            patch.object(reg, "is_saturated", return_value=False),
            patch.object(learner, "_deploy") as mock_deploy,
        ):
            learner._maybe_deploy()
            mock_deploy.assert_not_called()


def test_maybe_deploy_deploys_when_above_threshold() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("t1", 0.80, ["feat_speed"])
        reg.activate(1)
        # deployed_ndcg = 0.0, active = 0.80, threshold = 0.005 → 0.80 > 0.005 → deploy
        learner = _make_learner(registry=reg, deploy_threshold=0.005)
        with (
            patch.object(reg, "is_saturated", return_value=False),
            patch.object(learner, "_evaluate_blind_holdout", return_value=0.80),
            patch.object(learner, "_deploy") as mock_deploy,
        ):
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
        with (
            patch.object(reg, "is_saturated", return_value=False),
            patch.object(learner, "_evaluate_blind_holdout", return_value=0.505),
            patch.object(learner, "_deploy") as mock_deploy,
        ):
            learner._maybe_deploy()
            mock_deploy.assert_called_once()


def test_maybe_deploy_skips_when_blind_holdout_not_confirmed() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("t1", 0.80, ["feat_speed"])
        reg.activate(1)
        # active 0.80 vs deployed 0.0 passes first gate, but blind 0.0 fails it
        learner = _make_learner(registry=reg, deploy_threshold=0.005)
        with (
            patch.object(reg, "is_saturated", return_value=False),
            patch.object(learner, "_evaluate_blind_holdout", return_value=0.0),
            patch.object(learner, "_deploy") as mock_deploy,
        ):
            learner._maybe_deploy()
            mock_deploy.assert_not_called()


def test_maybe_deploy_deploys_when_blind_holdout_confirms() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("t1", 0.80, ["feat_speed"])
        reg.activate(1)
        # active 0.80, deployed 0.0, blind 0.79 → blind delta 0.79 >= threshold
        learner = _make_learner(registry=reg, deploy_threshold=0.005)
        with (
            patch.object(reg, "is_saturated", return_value=False),
            patch.object(learner, "_evaluate_blind_holdout", return_value=0.79),
            patch.object(learner, "_deploy") as mock_deploy,
        ):
            learner._maybe_deploy()
            mock_deploy.assert_called_once()


def test_maybe_deploy_skips_when_registry_saturated() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("t1", 0.80, ["feat_speed"])
        reg.activate(1)
        learner = _make_learner(registry=reg, deploy_threshold=0.005)
        with (
            patch.object(reg, "is_saturated", return_value=True),
            patch.object(learner, "_deploy") as mock_deploy,
        ):
            learner._maybe_deploy()
            mock_deploy.assert_not_called()


def test_maybe_deploy_returns_true_when_saturated() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("t1", 0.80, ["feat_speed"])
        reg.activate(1)
        learner = _make_learner(registry=reg)
        with patch.object(reg, "is_saturated", return_value=True):
            result = learner._maybe_deploy()
        assert result is True


def test_maybe_deploy_returns_false_when_no_active_entry() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with patch.object(reg, "is_saturated", return_value=False):
            result = learner._maybe_deploy()
        assert result is False


def test_maybe_deploy_returns_false_when_below_threshold() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("t1", 0.50, ["feat_speed"])
        reg.activate(1)
        reg.record_deployment(0.497, 1)
        learner = _make_learner(registry=reg, deploy_threshold=0.005)
        with (
            patch.object(reg, "is_saturated", return_value=False),
            patch.object(learner, "_deploy"),
        ):
            result = learner._maybe_deploy()
        assert result is False


def test_maybe_deploy_returns_false_when_blind_holdout_not_confirmed() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("t1", 0.80, ["feat_speed"])
        reg.activate(1)
        learner = _make_learner(registry=reg, deploy_threshold=0.005)
        with (
            patch.object(reg, "is_saturated", return_value=False),
            patch.object(learner, "_evaluate_blind_holdout", return_value=0.0),
            patch.object(learner, "_deploy"),
        ):
            result = learner._maybe_deploy()
        assert result is False


def test_maybe_deploy_returns_false_after_successful_deploy() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("t1", 0.80, ["feat_speed"])
        reg.activate(1)
        learner = _make_learner(registry=reg, deploy_threshold=0.005)
        with (
            patch.object(reg, "is_saturated", return_value=False),
            patch.object(learner, "_evaluate_blind_holdout", return_value=0.80),
            patch.object(learner, "_deploy") as mock_deploy,
        ):
            result = learner._maybe_deploy()
        mock_deploy.assert_called_once()
        assert result is False


def test_evaluate_blind_holdout_calls_evaluate_feature_set_with_holdout_year() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, blind_holdout_year=2025)
        entry = _make_entry(feature_names=["feat_speed"])
        with patch(
            "learning.continuous_learner.evaluate_feature_set",
            return_value=0.5,
        ) as mock_eval:
            result = learner._evaluate_blind_holdout(entry)
        assert result == pytest.approx(0.5)
        assert mock_eval.call_args.args[1] == ["feat_speed"]
        assert mock_eval.call_args.args[2] == [2025]


# ---------------------------------------------------------------------------
# _deploy
# ---------------------------------------------------------------------------


def test_deploy_calls_pipeline_steps_in_correct_order() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, docker_build=True)
        entry = _make_entry(ndcg=0.75, feature_names=["feat_speed"])
        order: list[str] = []

        with (
            patch(
                "learning.continuous_learner.write_filtered_parquet",
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
                side_effect=lambda *a, **kw: (order.append("stage"), Path("/tmp/staged"))[1],
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
                "learning.continuous_learner.write_filtered_parquet",
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


def test_deploy_rollback_triggered_when_record_deployment_raises(tmp_path: Path) -> None:
    """record_deployment runs inside the try block so that if it fails,
    staged artifacts are rolled back via _rollback_deploy."""
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, repo_root=tmp_path)
        entry = _make_entry(ndcg=0.77, feature_names=["feat_a"])
        staged_dir = tmp_path / "staged"
        staged_dir.mkdir()

        with (
            patch(
                "learning.continuous_learner.write_filtered_parquet",
                return_value=Path("/tmp/f.parquet"),
            ),
            patch.object(
                learner, "_train_production_model", return_value=Path("/tmp/model")
            ),
            patch.object(
                learner, "_stage_model", return_value=staged_dir
            ),
            patch.object(learner, "_update_model_meta_json", return_value=None),
            patch.object(learner, "_rebuild_docker"),
            patch.object(
                reg, "record_deployment", side_effect=RuntimeError("db write fail")
            ),
            patch.object(learner, "_rollback_deploy") as mock_rollback,
        ):
            with pytest.raises(RuntimeError, match="db write fail"):
                learner._deploy(entry)

        mock_rollback.assert_called_once()


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


def test_init_raises_for_unknown_category() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        with pytest.raises(ValueError, match="unknown-cat"):
            _make_learner(registry=reg, category="unknown-cat")


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


def test_training_scripts_exist_under_module_parent_dir() -> None:
    module_dir = Path(subject.__file__).parent.parent
    assert (module_dir / "train_finish_position_catboost_walk_forward.py").is_file()
    assert (module_dir / "train_finish_position_xgboost_walk_forward.py").is_file()


def test_main_resolves_training_script_to_existing_file(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().write_parquet(parquet_path)
    registry_path = tmp_path / "reg.duckdb"

    captured_scripts_dir: list[Path] = []
    original_init = cast("Callable[..., None]", subject.ContinuousLearner.__init__)

    def capturing_init(
        self: subject.ContinuousLearner, *args: object, **kwargs: object
    ) -> None:
        original_init(self, *args, **kwargs)
        captured_scripts_dir.append(cast("Path", kwargs["scripts_dir"]))

    with (
        patch.object(subject.ContinuousLearner, "__init__", capturing_init),
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
            ]
        )

    scripts_dir = captured_scripts_dir[0]
    script_name = subject._TRAINING_SCRIPT["jra"]
    assert (scripts_dir / script_name).is_file()


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


def test_stage_model_raises_file_not_found_when_model_json_missing(tmp_path: Path) -> None:
    model_dir = tmp_path / "model_dir"
    model_dir.mkdir()
    # model.json intentionally not created

    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="jra", repo_root=tmp_path)
        with pytest.raises(FileNotFoundError, match="model.json"):
            learner._stage_model(model_dir, ["feat_speed"], "auto-jra-v1")


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


def test_rebuild_docker_passes_timeout_to_subprocess() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, repo_root=Path("/repo"))
        with patch("subprocess.run") as mock_run:
            learner._rebuild_docker()
        assert mock_run.call_args.kwargs["timeout"] == subject.DEFAULT_DOCKER_BUILD_TIMEOUT_S


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
        assert isinstance(root.handlers[0], logging.StreamHandler)
        assert root.handlers[0].stream is sys.stdout
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
    _make_df().write_parquet(parquet_path)
    registry_path = tmp_path / "reg.duckdb"

    explore_calls: list[int] = []

    def fake_explore(round_num: int, n_trials: int) -> None:
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


def test_main_wires_per_trial_timeout_into_learner(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().write_parquet(parquet_path)
    registry_path = tmp_path / "reg.duckdb"
    captured: dict[str, float | None] = {}

    def capture_timeout(self: subject.ContinuousLearner, max_rounds: int | None = None) -> None:
        _ = max_rounds
        captured["per_trial_timeout_s"] = self._per_trial_timeout_s

    with patch.object(subject.ContinuousLearner, "run", capture_timeout):
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
                "--per-trial-timeout",
                "90.0",
            ]
        )

    assert captured["per_trial_timeout_s"] == pytest.approx(90.0)


def test_main_default_constants() -> None:
    assert subject.DEFAULT_DOCKER_TAG == "finish-position-predict-local:split2"
    assert subject.DEFAULT_DEPLOY_THRESHOLD == 0.005
    assert subject.DEFAULT_N_TRIALS == 20


def test_main_wires_trial_counts_into_controller(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().write_parquet(parquet_path)
    registry_path = tmp_path / "reg.duckdb"

    created_controllers: list[subject.AdaptiveLoadController] = []
    original_init = cast(
        "Callable[..., None]", subject.AdaptiveLoadController.__init__
    )

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
                "--n-trials",
                "15",
                "--min-trials",
                "3",
                "--max-trials",
                "40",
            ]
        )

    assert len(created_controllers) == 1
    ctrl = created_controllers[0]
    assert ctrl._base_n_trials == 15
    assert ctrl._min_n_trials == 3
    assert ctrl._max_n_trials == 40


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


def test_adaptive_controller_cpu_percent_returns_zero_without_psutil() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=20)
    with patch.object(subject, "_psutil", None):
        result = ctrl._cpu_percent()
    assert result == 0.0


def test_adaptive_controller_cpu_percent_returns_float_when_psutil_available() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=20)
    fake_psutil = MagicMock()
    fake_psutil.cpu_percent.return_value = 42.5
    with patch.object(subject, "_psutil", fake_psutil):
        result = ctrl._cpu_percent()
    assert result == 42.5


def test_adaptive_controller_cpu_percent_uses_non_blocking_interval() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=20)
    fake_psutil = MagicMock()
    fake_psutil.cpu_percent.return_value = 30.0
    with patch.object(subject, "_psutil", fake_psutil):
        ctrl._cpu_percent()
    assert fake_psutil.cpu_percent.call_args.kwargs["interval"] is None


def test_adaptive_controller_primes_cpu_counter_on_construction() -> None:
    fake_psutil = MagicMock()
    fake_psutil.cpu_percent.return_value = 0.0
    with patch.object(subject, "_psutil", fake_psutil):
        subject.AdaptiveLoadController(base_n_trials=20)
    fake_psutil.cpu_percent.assert_called_once_with(interval=None)


def test_adaptive_controller_construction_tolerates_missing_psutil() -> None:
    with patch.object(subject, "_psutil", None):
        ctrl = subject.AdaptiveLoadController(base_n_trials=20)
    assert ctrl._base_n_trials == 20


def test_adaptive_controller_mem_percent_returns_zero_without_psutil() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=20)
    with patch.object(subject, "_psutil", None):
        result = ctrl._mem_percent()
    assert result == 0.0


def test_adaptive_controller_mem_percent_returns_float_when_psutil_available() -> None:
    ctrl = subject.AdaptiveLoadController(base_n_trials=20)
    fake_psutil = MagicMock()
    fake_psutil.virtual_memory.return_value = MagicMock(percent=63.0)
    with patch.object(subject, "_psutil", fake_psutil):
        result = ctrl._mem_percent()
    assert result == 63.0


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
            patch("learning.continuous_learner.time.sleep") as mock_sleep,
        ):
            learner.run(max_rounds=1)
        mock_sleep.assert_called_once_with(5.0)


def test_run_without_controller_no_sleep() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, load_controller=None)
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy"),
            patch("learning.continuous_learner.time.sleep") as mock_sleep,
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
        with patch("learning.continuous_learner.shutil.rmtree", side_effect=OSError("cannot remove")):
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
        learner = _make_learner(registry=reg, docker_build=True)
        entry = _make_entry(ndcg=0.75, feature_names=["feat_speed"])

        staged_path = tmp_path / "staged"

        with (
            patch(
                "learning.continuous_learner.write_filtered_parquet",
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
                "learning.continuous_learner.write_filtered_parquet",
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
                "learning.continuous_learner.write_filtered_parquet",
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


def test_rollback_deploy_skips_staged_removal_when_staged_dest_is_none(tmp_path: Path) -> None:
    json_path = tmp_path / subject._MODEL_META_JSON_PATH
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text('{"current": true}', encoding="utf-8")

    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, repo_root=tmp_path)
        learner._rollback_deploy(None, None)

    assert json_path.read_text(encoding="utf-8") == '{"current": true}'


def test_train_production_model_passes_timeout_to_subprocess(tmp_path: Path) -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, category="jra", validation_years=[2024])
        with patch("subprocess.run") as mock_run:
            learner._train_production_model(
                Path("/p/features.parquet"), tmp_path / "models", "auto-jra-v1"
            )
    assert mock_run.call_args.kwargs["timeout"] == subject.DEFAULT_TRAINING_TIMEOUT_S


def test_rollback_deploy_logs_error_when_rmtree_fails(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    staged = tmp_path / "staged_model"
    staged.mkdir()
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, repo_root=tmp_path)
        with patch("learning.continuous_learner.shutil.rmtree", side_effect=OSError("disk error")):
            import logging
            with caplog.at_level(logging.ERROR, logger="learning.continuous_learner"):
                learner._rollback_deploy(staged, None)
    assert any("failed to remove staged dir" in r.message for r in caplog.records)
    assert all(r.levelno == logging.ERROR for r in caplog.records if "failed to remove staged dir" in r.message)


def test_rollback_deploy_logs_error_when_write_fails(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, repo_root=tmp_path)
        with patch.object(Path, "write_text", side_effect=OSError("disk full")):
            import logging
            with caplog.at_level(logging.ERROR, logger="learning.continuous_learner"):
                learner._rollback_deploy(None, '{"original": true}')
    assert any("failed to restore model_meta.json" in r.message for r in caplog.records)
    assert all(r.levelno == logging.ERROR for r in caplog.records if "failed to restore model_meta.json" in r.message)


# ---------------------------------------------------------------------------
# _check_and_try_inverses / _run_inverse_exploration
# ---------------------------------------------------------------------------


def test_strong_negative_threshold_constant() -> None:
    assert subject.STRONG_NEGATIVE_THRESHOLD_PP == -1.0


def test_max_inverse_per_round_constant() -> None:
    assert subject.MAX_INVERSE_PER_ROUND == 3


def test_inverse_approach_types_constant() -> None:
    from learning import feature_registry

    assert feature_registry.INVERSE_APPROACH_TYPES == (
        "feature_negate",
        "weight_invert",
        "window_invert",
        "anti_correlation",
    )


def test_learning_package_lazy_reexports_feature_registry_symbol() -> None:
    import learning

    from learning.feature_registry import FeatureRegistry as DirectFeatureRegistry

    assert learning.FeatureRegistry is DirectFeatureRegistry


def test_learning_package_getattr_raises_for_unknown_symbol() -> None:
    import learning

    with pytest.raises(AttributeError, match="has no attribute 'NoSuchSymbol'"):
        _ = learning.NoSuchSymbol


def test_check_and_try_inverses_does_nothing_when_no_negative_trials() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with patch.object(learner, "_run_inverse_exploration") as mock_run:
            learner._check_and_try_inverses(0, 20)
            mock_run.assert_not_called()


def test_check_and_try_inverses_skips_already_tried() -> None:
    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.list_strongly_negative_trials.return_value = [
        _make_entry(ndcg=0.4, feature_names=["feat_speed"])
    ]
    mock_registry.has_inverse_been_tried.return_value = True
    learner = _make_learner(registry=mock_registry)
    with patch.object(learner, "_run_inverse_exploration") as mock_run:
        learner._check_and_try_inverses(0, 20)
    mock_run.assert_not_called()
    mock_registry.record_inverse_trial.assert_not_called()
    assert mock_registry.has_inverse_been_tried.call_count == 4


def test_check_and_try_inverses_runs_new_inverse_for_each_approach() -> None:
    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.list_strongly_negative_trials.return_value = [
        _make_entry(ndcg=0.4, feature_names=["feat_speed"])
    ]
    mock_registry.has_inverse_been_tried.return_value = False
    learner = _make_learner(registry=mock_registry)
    with patch.object(
        learner,
        "_run_inverse_exploration",
        return_value={"delta_pp": {"ndcg_delta": 0.01}, "decision": "ADOPT"},
    ) as mock_run:
        learner._check_and_try_inverses(2, 20)
    assert mock_run.call_count == subject.MAX_INVERSE_PER_ROUND
    assert mock_registry.record_inverse_trial.call_count == subject.MAX_INVERSE_PER_ROUND


def _make_negative_trials(count: int) -> list[FeatureEntry]:
    return [
        FeatureEntry(
            id=i,
            trial_id=f"neg-trial-{i}",
            ndcg_at_3=0.40,
            is_active=False,
            feature_names=["feat_speed"],
            definition_json="{}",
            created_at="2026-01-01T00:00:00+00:00",
        )
        for i in range(count)
    ]


def test_check_and_try_inverses_caps_attempts_at_max_per_round() -> None:
    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.list_strongly_negative_trials.return_value = _make_negative_trials(10)
    mock_registry.has_inverse_been_tried.return_value = False
    learner = _make_learner(registry=mock_registry)
    with patch.object(
        learner,
        "_run_inverse_exploration",
        return_value={"delta_pp": {"ndcg_delta": 0.01}, "decision": "ADOPT"},
    ) as mock_run:
        learner._check_and_try_inverses(0, 20)
    assert mock_run.call_count == subject.MAX_INVERSE_PER_ROUND
    assert mock_registry.record_inverse_trial.call_count == subject.MAX_INVERSE_PER_ROUND


def test_check_and_try_inverses_logs_cap_reached_message(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.list_strongly_negative_trials.return_value = _make_negative_trials(10)
    mock_registry.has_inverse_been_tried.return_value = False
    learner = _make_learner(registry=mock_registry)
    with patch.object(
        learner,
        "_run_inverse_exploration",
        return_value={"delta_pp": {"ndcg_delta": 0.01}, "decision": "ADOPT"},
    ):
        with caplog.at_level(logging.INFO, logger="learning.continuous_learner"):
            learner._check_and_try_inverses(0, 20)
    assert any("inverse cap reached" in r.message for r in caplog.records)


def test_check_and_try_inverses_records_inverse_name_and_decision() -> None:
    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.list_strongly_negative_trials.return_value = [
        _make_entry(ndcg=0.4, feature_names=["feat_speed"])
    ]
    mock_registry.has_inverse_been_tried.return_value = False
    learner = _make_learner(registry=mock_registry)
    with patch.object(
        learner,
        "_run_inverse_exploration",
        return_value={"delta_pp": {"ndcg_delta": -0.02}, "decision": "REJECT"},
    ):
        learner._check_and_try_inverses(0, 20)
    first_call = mock_registry.record_inverse_trial.call_args_list[0]
    assert first_call.kwargs["original_trial_id"] == "trial-x"
    assert first_call.kwargs["inverse_name"] == "trial-x__feature_negate"
    assert first_call.kwargs["approach_type"] == "feature_negate"
    assert first_call.kwargs["decision"] == "REJECT"
    assert first_call.kwargs["delta_pp"] == {"ndcg_delta": -0.02}


def test_check_and_try_inverses_dedup_uses_full_inverse_name() -> None:
    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.list_strongly_negative_trials.return_value = [
        _make_entry(ndcg=0.4, feature_names=["feat_speed"])
    ]
    mock_registry.has_inverse_been_tried.return_value = True
    learner = _make_learner(registry=mock_registry)
    with patch.object(learner, "_run_inverse_exploration"):
        learner._check_and_try_inverses(0, 20)
    tried_names = [c.args[1] for c in mock_registry.has_inverse_been_tried.call_args_list]
    assert tried_names == [
        "trial-x__feature_negate",
        "trial-x__weight_invert",
        "trial-x__window_invert",
        "trial-x__anti_correlation",
    ]


def test_inverse_n_trials_constant() -> None:
    assert subject.INVERSE_N_TRIALS == 2


def test_run_inverse_exploration_uses_inverse_n_trials_constant() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg,
            validation_year_pool=[2021, 2022, 2023],
            blind_holdout_year=2023,
        )
        trial = _make_entry(ndcg=0.4, feature_names=["feat_speed"])
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._run_inverse_exploration(trial, "feature_negate", 1, 20)
        kwargs = mock_run.call_args.kwargs
        assert kwargs["n_trials"] == 2
        assert kwargs["registry"] is reg
        years = kwargs["validation_years"]
        assert len(years) == 1
        assert 2023 not in years
        assert set(years).issubset({2021, 2022})


def test_run_inverse_exploration_screens_on_single_validation_year() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg,
            validation_year_pool=[2021, 2022, 2023],
            blind_holdout_year=2023,
        )
        trial = _make_entry(ndcg=0.4, feature_names=["feat_speed"])
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._run_inverse_exploration(trial, "feature_negate", 1, 20)
        full_round_years = select_round_validation_years(
            1, [2021, 2022, 2023], 2023
        )
        screen_years = mock_run.call_args.kwargs["validation_years"]
        assert screen_years == full_round_years[:1]


def test_run_inverse_exploration_passes_screening_true() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        trial = _make_entry(ndcg=0.4, feature_names=["feat_speed"])
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._run_inverse_exploration(trial, "feature_negate", 1, 20)
        assert mock_run.call_args.kwargs["screening"] is True


def test_run_inverse_exploration_n_trials_ignores_caller_value() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        trial = _make_entry(ndcg=0.4, feature_names=["feat_speed"])
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._run_inverse_exploration(trial, "feature_negate", 1, 2)
        assert mock_run.call_args.kwargs["n_trials"] == 2


def test_run_inverse_exploration_study_name_includes_approach_and_trial() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        trial = _make_entry(ndcg=0.4, feature_names=["feat_speed"])
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._run_inverse_exploration(trial, "weight_invert", 3, 20)
        assert mock_run.call_args.kwargs["study_name"] == "inv-weight_invert-trial-x-r3"


def _record_study_trial(
    reg: FeatureRegistry, ndcg: float
) -> Callable[..., None]:
    """Build a run_exploration side-effect that records one trial for the run's study.

    The real run_exploration writes trials named ``{study_name}_trial_{n}``; mocking it
    out means get_best_ndcg_for_study would find nothing, so the side-effect inserts a
    matching trial to simulate the run producing that score.
    """

    def _side_effect(*_: object, **kwargs: object) -> None:
        study_name = cast(str, kwargs["study_name"])
        reg.record_trial(f"{study_name}_trial_0", ndcg, ["feat_speed"])

    return _side_effect


def test_run_inverse_exploration_returns_adopt_when_study_best_beats_pre_active() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("active", 0.50, ["feat_speed"])
        reg.activate(1)
        learner = _make_learner(registry=reg)
        trial = _make_entry(ndcg=0.4, feature_names=["feat_speed"])
        with patch(
            "learning.continuous_learner.run_exploration",
            side_effect=_record_study_trial(reg, 0.60),
        ):
            result = learner._run_inverse_exploration(trial, "feature_negate", 0, 20)
        assert result["decision"] == "ADOPT"
        assert result["delta_pp"] == {"ndcg_delta": pytest.approx(0.10)}


def test_run_inverse_exploration_returns_reject_when_study_best_below_pre_active() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("active", 0.60, ["feat_speed"])
        reg.activate(1)
        learner = _make_learner(registry=reg)
        trial = _make_entry(ndcg=0.4, feature_names=["feat_speed"])
        with patch(
            "learning.continuous_learner.run_exploration",
            side_effect=_record_study_trial(reg, 0.45),
        ):
            result = learner._run_inverse_exploration(trial, "feature_negate", 0, 20)
        assert result["decision"] == "REJECT"
        assert result["delta_pp"] == {"ndcg_delta": pytest.approx(-0.15)}


def test_run_inverse_exploration_rejects_when_study_best_equals_pre_active() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("active", 0.60, ["feat_speed"])
        reg.activate(1)
        learner = _make_learner(registry=reg)
        trial = _make_entry(ndcg=0.4, feature_names=["feat_speed"])
        with patch(
            "learning.continuous_learner.run_exploration",
            side_effect=_record_study_trial(reg, 0.60),
        ):
            result = learner._run_inverse_exploration(trial, "feature_negate", 0, 20)
        assert result["decision"] == "REJECT"
        assert result["delta_pp"] == {"ndcg_delta": pytest.approx(0.0)}


def test_run_inverse_exploration_uses_zero_pre_active_when_none_active() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("inactive", 0.30, ["feat_speed"])
        learner = _make_learner(registry=reg)
        trial = _make_entry(ndcg=0.4, feature_names=["feat_speed"])
        with patch(
            "learning.continuous_learner.run_exploration",
            side_effect=_record_study_trial(reg, 0.30),
        ):
            result = learner._run_inverse_exploration(trial, "feature_negate", 0, 20)
        assert result["decision"] == "ADOPT"
        assert result["delta_pp"] == {"ndcg_delta": pytest.approx(0.30)}


def test_run_inverse_exploration_rejects_when_study_produced_no_trials() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("active", 0.50, ["feat_speed"])
        reg.activate(1)
        learner = _make_learner(registry=reg)
        trial = _make_entry(ndcg=0.4, feature_names=["feat_speed"])
        with patch("learning.continuous_learner.run_exploration"):
            result = learner._run_inverse_exploration(trial, "feature_negate", 0, 20)
        assert result["decision"] == "REJECT"
        assert result["delta_pp"] == {"ndcg_delta": pytest.approx(0.0)}


def test_run_inverse_exploration_ignores_other_studies_and_global_best() -> None:
    # A pre-existing high-NDCG trial from a DIFFERENT study must not leak into the
    # delta: the gain is measured only from this inverse run's own trials.
    with FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("active", 0.50, ["feat_speed"])
        reg.activate(1)
        reg.record_trial("inv-other-trial-x-r0_trial_0", 0.95, ["feat_speed"])
        learner = _make_learner(registry=reg)
        trial = _make_entry(ndcg=0.4, feature_names=["feat_speed"])
        with patch(
            "learning.continuous_learner.run_exploration",
            side_effect=_record_study_trial(reg, 0.52),
        ):
            result = learner._run_inverse_exploration(trial, "feature_negate", 0, 20)
        assert result["decision"] == "ADOPT"
        assert result["delta_pp"] == {"ndcg_delta": pytest.approx(0.02)}


def test_inverse_trials_get_distinct_per_trial_deltas() -> None:
    # The core bug: every inverse trial reported the SAME delta. With the fix each
    # inverse run's delta reflects its own study's best minus the pre-run active.
    with FeatureRegistry(Path(":memory:")) as reg:
        reg.record_trial("active", 0.50, ["feat_speed"])
        reg.activate(1)
        learner = _make_learner(registry=reg)
        trial = _make_entry(ndcg=0.4, feature_names=["feat_speed"])
        study_scores = {
            "feature_negate": 0.55,
            "weight_invert": 0.48,
            "window_invert": 0.61,
        }

        def _side_effect(*_: object, **kwargs: object) -> None:
            study_name = cast(str, kwargs["study_name"])
            approach = study_name.split("-")[1]
            reg.record_trial(
                f"{study_name}_trial_0", study_scores[approach], ["feat_speed"]
            )

        deltas: list[float] = []
        with patch(
            "learning.continuous_learner.run_exploration", side_effect=_side_effect
        ):
            for approach in study_scores:
                outcome = learner._run_inverse_exploration(trial, approach, 0, 20)
                deltas.append(outcome["delta_pp"]["ndcg_delta"])

        assert deltas == [
            pytest.approx(0.05),
            pytest.approx(-0.02),
            pytest.approx(0.11),
        ]
        assert len(set(deltas)) == 3


def test_run_calls_check_inverses_after_each_round() -> None:
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
            patch.object(
                learner,
                "_check_and_try_inverses",
                side_effect=lambda *a, **kw: order.append("inverse"),
            ),
        ):
            learner.run(max_rounds=2)
        assert order == ["explore", "deploy", "inverse", "explore", "deploy", "inverse"]


def test_run_passes_actual_trials_to_check_inverses() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        ctrl = MagicMock(spec=subject.AdaptiveLoadController)
        ctrl.round_params.return_value = (15, 0.0)
        learner = _make_learner(
            registry=reg, n_trials_per_round=20, load_controller=ctrl
        )
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy", return_value=False),
            patch.object(learner, "_check_and_try_inverses") as mock_check,
            patch.object(learner, "_analyze_feature_enrichment"),
        ):
            learner.run(max_rounds=1)
        mock_check.assert_called_once_with(0, 15)


# ---------------------------------------------------------------------------
# _analyze_feature_enrichment / _run_enrichment_trial
# ---------------------------------------------------------------------------


def test_enrichment_constants() -> None:
    assert subject.ENRICHMENT_THRESHOLD == 0.3
    assert subject.MAX_ENRICHMENT_FEATURES == 5


def test_run_calls_analyze_enrichment_after_inverses() -> None:
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
            patch.object(
                learner,
                "_check_and_try_inverses",
                side_effect=lambda *a, **kw: order.append("inverse"),
            ),
            patch.object(
                learner,
                "_analyze_feature_enrichment",
                side_effect=lambda *a, **kw: order.append("enrich"),
            ),
        ):
            learner.run(max_rounds=1)
        assert order == ["explore", "deploy", "inverse", "enrich"]


def test_run_passes_round_num_to_analyze_enrichment() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy", return_value=False),
            patch.object(learner, "_check_and_try_inverses"),
            patch.object(learner, "_analyze_feature_enrichment") as mock_enrich,
        ):
            learner.run(max_rounds=1)
        mock_enrich.assert_called_once_with(0)


def test_analyze_feature_enrichment_logs_nothing_found_when_empty(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.compute_feature_enrichment.return_value = []
    learner = _make_learner(registry=mock_registry)
    with patch.object(learner, "_run_enrichment_trial") as mock_trial:
        with caplog.at_level(logging.INFO, logger="learning.continuous_learner"):
            learner._analyze_feature_enrichment(0)
    assert any("no enriched features found" in r.message for r in caplog.records)
    mock_trial.assert_not_called()


def test_analyze_feature_enrichment_logs_candidates(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.compute_feature_enrichment.return_value = [("feat_new", 0.8)]
    mock_registry.get_active_entry.return_value = _make_entry(
        feature_names=["feat_speed"]
    )
    learner = _make_learner(registry=mock_registry)
    with patch.object(learner, "_run_enrichment_trial"):
        with caplog.at_level(logging.INFO, logger="learning.continuous_learner"):
            learner._analyze_feature_enrichment(0)
    assert any(
        "enriched feature: feat_new score=0.800" in r.message for r in caplog.records
    )


def test_analyze_feature_enrichment_runs_trial_when_candidates_found() -> None:
    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.compute_feature_enrichment.return_value = [("feat_new", 0.8)]
    mock_registry.get_active_entry.return_value = _make_entry(
        feature_names=["feat_speed"]
    )
    learner = _make_learner(registry=mock_registry)
    with patch.object(learner, "_run_enrichment_trial") as mock_trial:
        learner._analyze_feature_enrichment(4)
    mock_trial.assert_called_once_with({"feat_speed"}, [("feat_new", 0.8)], 4)


def test_analyze_feature_enrichment_uses_cached_enrichment() -> None:
    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.get_active_entry.return_value = _make_entry(
        feature_names=["feat_speed"]
    )
    learner = _make_learner(registry=mock_registry)
    learner._last_enrichment = [("feat_cached", 0.7)]
    with patch.object(learner, "_run_enrichment_trial") as mock_trial:
        learner._analyze_feature_enrichment(0)
    mock_registry.compute_feature_enrichment.assert_not_called()
    mock_trial.assert_called_once_with({"feat_speed"}, [("feat_cached", 0.7)], 0)


def test_analyze_feature_enrichment_clears_cache_after_use() -> None:
    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.get_active_entry.return_value = _make_entry(
        feature_names=["feat_speed"]
    )
    learner = _make_learner(registry=mock_registry)
    learner._last_enrichment = [("feat_cached", 0.7)]
    with patch.object(learner, "_run_enrichment_trial"):
        learner._analyze_feature_enrichment(0)
    assert learner._last_enrichment is None


def test_analyze_feature_enrichment_computes_when_cache_empty() -> None:
    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.compute_feature_enrichment.return_value = [("feat_new", 0.8)]
    mock_registry.get_active_entry.return_value = _make_entry(
        feature_names=["feat_speed"]
    )
    learner = _make_learner(registry=mock_registry)
    assert learner._last_enrichment is None
    with patch.object(learner, "_run_enrichment_trial") as mock_trial:
        learner._analyze_feature_enrichment(0)
    mock_registry.compute_feature_enrichment.assert_called_once_with()
    mock_trial.assert_called_once_with({"feat_speed"}, [("feat_new", 0.8)], 0)


def test_analyze_feature_enrichment_skips_trial_when_no_active_entry() -> None:
    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.compute_feature_enrichment.return_value = [("feat_new", 0.8)]
    mock_registry.get_active_entry.return_value = None
    learner = _make_learner(registry=mock_registry)
    with patch.object(learner, "_run_enrichment_trial") as mock_trial:
        learner._analyze_feature_enrichment(0)
    mock_trial.assert_not_called()


def test_analyze_feature_enrichment_skips_features_already_active() -> None:
    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.compute_feature_enrichment.return_value = [("feat_speed", 0.8)]
    mock_registry.get_active_entry.return_value = _make_entry(
        feature_names=["feat_speed"]
    )
    learner = _make_learner(registry=mock_registry)
    with patch.object(learner, "_run_enrichment_trial") as mock_trial:
        learner._analyze_feature_enrichment(0)
    mock_trial.assert_not_called()


def test_analyze_feature_enrichment_skips_negative_scores() -> None:
    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.compute_feature_enrichment.return_value = [("feat_bad", -0.8)]
    mock_registry.get_active_entry.return_value = _make_entry(
        feature_names=["feat_speed"]
    )
    learner = _make_learner(registry=mock_registry)
    with patch.object(learner, "_run_enrichment_trial") as mock_trial:
        learner._analyze_feature_enrichment(0)
    mock_trial.assert_not_called()


def test_analyze_feature_enrichment_caps_candidates_to_max() -> None:
    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.compute_feature_enrichment.return_value = [
        (f"feat_new_{i}", 0.9 - i * 0.05) for i in range(8)
    ]
    mock_registry.get_active_entry.return_value = _make_entry(
        feature_names=["feat_speed"]
    )
    learner = _make_learner(registry=mock_registry)
    with patch.object(learner, "_run_enrichment_trial") as mock_trial:
        learner._analyze_feature_enrichment(0)
    passed_candidates = mock_trial.call_args.args[1]
    assert len(passed_candidates) == subject.MAX_ENRICHMENT_FEATURES


def test_enrichment_n_trials_constant() -> None:
    assert subject.ENRICHMENT_N_TRIALS == 2


def test_run_enrichment_trial_uses_enrichment_n_trials_constant() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg,
            n_trials_per_round=20,
            validation_year_pool=[2021, 2022, 2023],
            blind_holdout_year=2023,
        )
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._run_enrichment_trial({"feat_speed"}, [("feat_new", 0.8)], 2)
        kwargs = mock_run.call_args.kwargs
        assert kwargs["n_trials"] == 2
        assert kwargs["registry"] is reg
        years = kwargs["validation_years"]
        assert len(years) == 1
        assert 2023 not in years
        assert set(years).issubset({2021, 2022})


def test_run_enrichment_trial_screens_on_single_validation_year() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg,
            validation_year_pool=[2021, 2022, 2023],
            blind_holdout_year=2023,
        )
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._run_enrichment_trial({"feat_x"}, [("feat_y", 1.0)], 2)
        full_round_years = select_round_validation_years(
            2, [2021, 2022, 2023], 2023
        )
        screen_years = mock_run.call_args.kwargs["validation_years"]
        assert screen_years == full_round_years[:1]


def test_run_enrichment_trial_uses_rotating_validation_years() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg,
            validation_year_pool=[2021, 2022, 2023],
            blind_holdout_year=2023,
        )
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._run_enrichment_trial({"feat_x"}, [("feat_y", 1.0)], 2)
        years = mock_run.call_args.kwargs["validation_years"]
        assert len(years) == 1
        assert 2023 not in years
        assert set(years).issubset({2021, 2022})


def test_run_enrichment_trial_validation_years_vary_across_rounds() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg,
            validation_year_pool=[2021, 2022, 2023, 2024, 2025],
            blind_holdout_year=2025,
        )
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._run_enrichment_trial({"feat_x"}, [("feat_y", 1.0)], 0)
            years_round_0 = mock_run.call_args.kwargs["validation_years"]
            learner._run_enrichment_trial({"feat_x"}, [("feat_y", 1.0)], 5)
            years_round_5 = mock_run.call_args.kwargs["validation_years"]
        assert years_round_0 != years_round_5


def test_run_enrichment_trial_n_trials_ignores_per_round_value() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, n_trials_per_round=40)
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._run_enrichment_trial({"feat_speed"}, [("feat_new", 0.8)], 0)
        assert mock_run.call_args.kwargs["n_trials"] == 2


def test_run_enrichment_trial_study_name_includes_round_and_features() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._run_enrichment_trial(
                {"feat_speed"},
                [("feat_a", 0.8), ("feat_b", 0.7), ("feat_c", 0.6), ("feat_d", 0.5)],
                3,
            )
        assert mock_run.call_args.kwargs["study_name"] == "enrichment-r3-feat_a+feat_b+feat_c"


def test_run_enrichment_trial_passes_screening_true() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with patch("learning.continuous_learner.run_exploration") as mock_run:
            learner._run_enrichment_trial({"feat_speed"}, [("feat_new", 0.8)], 0)
        assert mock_run.call_args.kwargs["screening"] is True


# ---------------------------------------------------------------------------
# docker_build flag (Change 2)
# ---------------------------------------------------------------------------


def test_deploy_skips_rebuild_docker_when_docker_build_false() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, docker_build=False)
        entry = _make_entry(ndcg=0.75, feature_names=["feat_speed"])

        with (
            patch(
                "learning.continuous_learner.write_filtered_parquet",
                return_value=Path("/tmp/f.parquet"),
            ),
            patch.object(
                learner, "_train_production_model", return_value=Path("/tmp/model")
            ),
            patch.object(learner, "_stage_model", return_value=Path("/tmp/staged")),
            patch.object(learner, "_update_model_meta_json", return_value=None),
            patch.object(learner, "_rebuild_docker") as mock_docker,
        ):
            learner._deploy(entry)

        mock_docker.assert_not_called()


def test_deploy_calls_rebuild_docker_when_docker_build_true() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, docker_build=True)
        entry = _make_entry(ndcg=0.75, feature_names=["feat_speed"])

        with (
            patch(
                "learning.continuous_learner.write_filtered_parquet",
                return_value=Path("/tmp/f.parquet"),
            ),
            patch.object(
                learner, "_train_production_model", return_value=Path("/tmp/model")
            ),
            patch.object(learner, "_stage_model", return_value=Path("/tmp/staged")),
            patch.object(learner, "_update_model_meta_json", return_value=None),
            patch.object(learner, "_rebuild_docker") as mock_docker,
        ):
            learner._deploy(entry)

        mock_docker.assert_called_once()


def test_docker_build_default_is_false() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        assert learner._docker_build is False


def test_docker_build_true_is_stored() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, docker_build=True)
        assert learner._docker_build is True


# ---------------------------------------------------------------------------
# _load_features_dataframe (Change 3)
# ---------------------------------------------------------------------------


def test_load_features_dataframe_single_file_reads_whole_file(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().write_parquet(parquet_path)
    result = subject._load_features_dataframe(parquet_path, "20160101")
    assert "feat_speed" in result.columns
    assert len(result) == len(_make_df())


def _write_year_partition(partition_dir: Path, year: int) -> None:
    year_dir = partition_dir / f"race_year={year}"
    year_dir.mkdir(parents=True, exist_ok=True)
    _make_df().drop("race_year").write_parquet(year_dir / "part-0.parquet")


def test_load_features_dataframe_directory_filters_by_min_year(tmp_path: Path) -> None:
    partition_dir = tmp_path / "partitioned"
    # train_start 20130101 → min_year = 2012, so the 2011 partition is pruned.
    _write_year_partition(partition_dir, 2011)
    _write_year_partition(partition_dir, 2012)
    _write_year_partition(partition_dir, 2013)
    result = subject._load_features_dataframe(partition_dir, "20130101")
    assert set(result["race_year"].unique().to_list()) == {2012, 2013}


def test_load_features_dataframe_directory_uses_train_start_minus_one(
    tmp_path: Path,
) -> None:
    partition_dir = tmp_path / "partitioned"
    # train_start 20060101 → min_year = 2005, so the 2004 partition is pruned.
    _write_year_partition(partition_dir, 2004)
    _write_year_partition(partition_dir, 2005)
    _write_year_partition(partition_dir, 2006)
    result = subject._load_features_dataframe(partition_dir, "20060101")
    assert set(result["race_year"].unique().to_list()) == {2005, 2006}


def test_load_features_dataframe_directory_promotes_mismatched_umaban_dtype(
    tmp_path: Path,
) -> None:
    partition_dir = tmp_path / "partitioned"
    int_year_dir = partition_dir / "race_year=2013"
    int_year_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "race_id": ["r1", "r1"],
            "umaban": pl.Series([1, 2], dtype=pl.Int32),
            "feat_speed": [0.1, 0.2],
        }
    ).write_parquet(int_year_dir / "part-0.parquet")
    float_year_dir = partition_dir / "race_year=2014"
    float_year_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "race_id": ["r2", "r2"],
            "umaban": pl.Series([3.0, 4.0], dtype=pl.Float64),
            "feat_speed": [0.3, 0.4],
        }
    ).write_parquet(float_year_dir / "part-0.parquet")
    result = subject._load_features_dataframe(partition_dir, "20140101")
    assert result["umaban"].dtype == pl.Float64
    assert set(result["race_year"].unique().to_list()) == {2013, 2014}


def test_load_features_dataframe_directory_prunes_old_years_on_dtype_mismatch(
    tmp_path: Path,
) -> None:
    partition_dir = tmp_path / "partitioned"
    old_year_dir = partition_dir / "race_year=2011"
    old_year_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "race_id": ["r1", "r1"],
            "umaban": pl.Series([1, 2], dtype=pl.Int32),
            "feat_speed": [0.1, 0.2],
        }
    ).write_parquet(old_year_dir / "part-0.parquet")
    recent_year_dir = partition_dir / "race_year=2013"
    recent_year_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "race_id": ["r2", "r2"],
            "umaban": pl.Series([3.0, 4.0], dtype=pl.Float64),
            "feat_speed": [0.3, 0.4],
        }
    ).write_parquet(recent_year_dir / "part-0.parquet")
    # train_start 20130101 → min_year = 2012, so the 2011 partition is pruned even
    # though the SchemaError fallback path scans every file before filtering.
    result = subject._load_features_dataframe(partition_dir, "20130101")
    assert set(result["race_year"].unique().to_list()) == {2013}


def test_load_features_dataframe_single_file_is_grouped_by_race_id(
    tmp_path: Path,
) -> None:
    parquet_path = tmp_path / "features.parquet"
    pl.DataFrame(
        {
            "race_id": ["B", "A", "B", "A"],
            "umaban": [2, 2, 1, 1],
            "race_year": [2024, 2024, 2024, 2024],
            "feat_speed": [0.3, 0.4, 0.1, 0.2],
        }
    ).write_parquet(parquet_path)
    result = subject._load_features_dataframe(parquet_path, "20240101")
    race_ids = result["race_id"].to_list()
    runs = result["race_id"].rle().len()
    assert race_ids == ["A", "A", "B", "B"]
    assert runs == result["race_id"].n_unique()
    assert result["umaban"].to_list() == [1, 2, 1, 2]


def test_load_features_dataframe_directory_is_grouped_by_race_id(
    tmp_path: Path,
) -> None:
    partition_dir = tmp_path / "partitioned"
    year_dir = partition_dir / "race_year=2024"
    year_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "race_id": ["B", "A", "B", "A"],
            "umaban": [2, 2, 1, 1],
            "feat_speed": [0.3, 0.4, 0.1, 0.2],
        }
    ).write_parquet(year_dir / "part-0.parquet")
    result = subject._load_features_dataframe(partition_dir, "20240101")
    race_ids = result["race_id"].to_list()
    runs = result["race_id"].rle().len()
    assert race_ids == ["A", "A", "B", "B"]
    assert runs == result["race_id"].n_unique()
    assert result["umaban"].to_list() == [1, 2, 1, 2]


# ---------------------------------------------------------------------------
# skip_inverse / skip_enrichment (Change 4)
# ---------------------------------------------------------------------------


def test_run_skips_check_inverses_when_skip_inverse_true() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, skip_inverse=True)
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy", return_value=False),
            patch.object(learner, "_check_and_try_inverses") as mock_check,
            patch.object(learner, "_analyze_feature_enrichment"),
        ):
            learner.run(max_rounds=1)
        mock_check.assert_not_called()


def test_run_skips_analyze_enrichment_when_skip_enrichment_true() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, skip_enrichment=True)
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy", return_value=False),
            patch.object(learner, "_check_and_try_inverses"),
            patch.object(learner, "_analyze_feature_enrichment") as mock_enrich,
        ):
            learner.run(max_rounds=1)
        mock_enrich.assert_not_called()


def test_run_still_calls_inverses_and_enrichment_by_default() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy", return_value=False),
            patch.object(learner, "_check_and_try_inverses") as mock_check,
            patch.object(learner, "_analyze_feature_enrichment") as mock_enrich,
        ):
            learner.run(max_rounds=1)
        mock_check.assert_called_once()
        mock_enrich.assert_called_once()


def test_run_skips_inverse_and_enrichment_when_saturated() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy", return_value=True),
            patch.object(learner, "_check_and_try_inverses") as mock_check,
            patch.object(learner, "_analyze_feature_enrichment") as mock_enrich,
        ):
            learner.run(max_rounds=1)
        mock_check.assert_not_called()
        mock_enrich.assert_not_called()


def test_run_runs_inverse_and_enrichment_when_not_saturated() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy", return_value=False),
            patch.object(learner, "_check_and_try_inverses") as mock_check,
            patch.object(learner, "_analyze_feature_enrichment") as mock_enrich,
        ):
            learner.run(max_rounds=1)
        mock_check.assert_called_once()
        mock_enrich.assert_called_once()


def test_run_skips_inverse_when_saturated_even_with_skip_flags_false() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg, skip_inverse=False, skip_enrichment=False
        )
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy", return_value=True),
            patch.object(learner, "_check_and_try_inverses") as mock_check,
            patch.object(learner, "_analyze_feature_enrichment") as mock_enrich,
        ):
            learner.run(max_rounds=1)
        mock_check.assert_not_called()
        mock_enrich.assert_not_called()


def test_run_logs_skip_message_when_saturated(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy", return_value=True),
            patch.object(learner, "_check_and_try_inverses"),
            patch.object(learner, "_analyze_feature_enrichment"),
            caplog.at_level("INFO", logger="learning.continuous_learner"),
        ):
            learner.run(max_rounds=1)
        skip_logs = [
            r.message
            for r in caplog.records
            if "skipping inverse and enrichment" in r.message
        ]
        assert len(skip_logs) == 1


def test_run_does_not_log_skip_message_when_not_saturated(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy", return_value=False),
            patch.object(learner, "_check_and_try_inverses"),
            patch.object(learner, "_analyze_feature_enrichment"),
            caplog.at_level("INFO", logger="learning.continuous_learner"),
        ):
            learner.run(max_rounds=1)
        skip_logs = [
            r.message
            for r in caplog.records
            if "skipping inverse and enrichment" in r.message
        ]
        assert skip_logs == []


def test_skip_inverse_default_is_false() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        assert learner._skip_inverse is False


def test_skip_enrichment_default_is_false() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        assert learner._skip_enrichment is False


def test_skip_inverse_true_is_stored() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, skip_inverse=True)
        assert learner._skip_inverse is True


def test_skip_enrichment_true_is_stored() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, skip_enrichment=True)
        assert learner._skip_enrichment is True


# ---------------------------------------------------------------------------
# _resolve_backends (Change 1)
# ---------------------------------------------------------------------------


def test_resolve_backends_returns_category_default_when_arg_none_jra() -> None:
    assert subject._resolve_backends(None, "jra") == ("catboost",)


def test_resolve_backends_returns_category_default_when_arg_none_nar() -> None:
    assert subject._resolve_backends(None, "nar") == ("xgboost",)


def test_resolve_backends_falls_back_to_default_for_unknown_category() -> None:
    from learning.feature_explorer import DEFAULT_BACKENDS

    assert subject._resolve_backends(None, "mystery") == DEFAULT_BACKENDS


def test_resolve_backends_parses_csv_into_tuple() -> None:
    assert subject._resolve_backends("catboost,xgboost", "jra") == (
        "catboost",
        "xgboost",
    )


def test_resolve_backends_strips_whitespace_from_tokens() -> None:
    assert subject._resolve_backends(" lightgbm , catboost ", "nar") == (
        "lightgbm",
        "catboost",
    )


def test_resolve_backends_raises_for_unknown_token() -> None:
    with pytest.raises(ValueError, match="Unknown backend"):
        subject._resolve_backends("catboost,bogus", "jra")


# ---------------------------------------------------------------------------
# _CATEGORY_TRAIN_START (Change 5)
# ---------------------------------------------------------------------------


def test_category_train_start_jra() -> None:
    assert subject._CATEGORY_TRAIN_START["jra"] == "20130101"


def test_category_train_start_nar() -> None:
    assert subject._CATEGORY_TRAIN_START["nar"] == "20060101"


def test_category_train_start_banei() -> None:
    assert subject._CATEGORY_TRAIN_START["ban-ei"] == "20110101"


# ---------------------------------------------------------------------------
# main() resolution of train_start and backends (Changes 1 & 5)
# ---------------------------------------------------------------------------


def _capture_learner_kwargs(
    captured: dict[str, object],
) -> Callable[..., None]:
    original_init = cast("Callable[..., None]", subject.ContinuousLearner.__init__)

    def capturing_init(
        self: subject.ContinuousLearner, *args: object, **kwargs: object
    ) -> None:
        original_init(self, *args, **kwargs)
        captured.update(kwargs)

    return capturing_init


def test_main_resolves_train_start_for_jra_when_omitted(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().write_parquet(parquet_path)
    registry_path = tmp_path / "reg.duckdb"
    captured: dict[str, object] = {}

    with (
        patch.object(
            subject.ContinuousLearner,
            "__init__",
            _capture_learner_kwargs(captured),
        ),
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
            ]
        )

    assert captured["train_start"] == "20130101"


def test_main_resolves_train_start_for_nar_when_omitted(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().write_parquet(parquet_path)
    registry_path = tmp_path / "reg.duckdb"
    captured: dict[str, object] = {}

    with (
        patch.object(
            subject.ContinuousLearner,
            "__init__",
            _capture_learner_kwargs(captured),
        ),
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
                "nar",
                "--repo-root",
                str(tmp_path),
                "--registry-path",
                str(registry_path),
                "--max-rounds",
                "1",
            ]
        )

    assert captured["train_start"] == "20060101"


def test_main_resolves_train_start_for_banei_when_omitted(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().write_parquet(parquet_path)
    registry_path = tmp_path / "reg.duckdb"
    captured: dict[str, object] = {}

    with (
        patch.object(
            subject.ContinuousLearner,
            "__init__",
            _capture_learner_kwargs(captured),
        ),
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
                "ban-ei",
                "--repo-root",
                str(tmp_path),
                "--registry-path",
                str(registry_path),
                "--max-rounds",
                "1",
            ]
        )

    assert captured["train_start"] == "20110101"


def test_main_train_start_arg_overrides_category_default(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().write_parquet(parquet_path)
    registry_path = tmp_path / "reg.duckdb"
    captured: dict[str, object] = {}

    with (
        patch.object(
            subject.ContinuousLearner,
            "__init__",
            _capture_learner_kwargs(captured),
        ),
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
                "--train-start",
                "20180101",
                "--max-rounds",
                "1",
            ]
        )

    assert captured["train_start"] == "20180101"


def test_main_resolves_backends_for_jra_when_omitted(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().write_parquet(parquet_path)
    registry_path = tmp_path / "reg.duckdb"
    captured: dict[str, object] = {}

    with (
        patch.object(
            subject.ContinuousLearner,
            "__init__",
            _capture_learner_kwargs(captured),
        ),
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
            ]
        )

    assert captured["backends"] == ("catboost",)


def test_main_backends_arg_overrides_category_default(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().write_parquet(parquet_path)
    registry_path = tmp_path / "reg.duckdb"
    captured: dict[str, object] = {}

    with (
        patch.object(
            subject.ContinuousLearner,
            "__init__",
            _capture_learner_kwargs(captured),
        ),
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
                "--backends",
                "catboost,xgboost",
                "--max-rounds",
                "1",
            ]
        )

    assert captured["backends"] == ("catboost", "xgboost")


def test_main_forwards_docker_build_and_skip_flags(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().write_parquet(parquet_path)
    registry_path = tmp_path / "reg.duckdb"
    captured: dict[str, object] = {}

    with (
        patch.object(
            subject.ContinuousLearner,
            "__init__",
            _capture_learner_kwargs(captured),
        ),
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
                "--docker-build",
                "--skip-inverse",
                "--skip-enrichment",
                "--max-rounds",
                "1",
            ]
        )

    assert captured["docker_build"] is True
    assert captured["skip_inverse"] is True
    assert captured["skip_enrichment"] is True


def test_main_docker_build_and_skip_flags_default_false(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().write_parquet(parquet_path)
    registry_path = tmp_path / "reg.duckdb"
    captured: dict[str, object] = {}

    with (
        patch.object(
            subject.ContinuousLearner,
            "__init__",
            _capture_learner_kwargs(captured),
        ),
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
            ]
        )

    assert captured["docker_build"] is False
    assert captured["skip_inverse"] is False
    assert captured["skip_enrichment"] is False


# ---------------------------------------------------------------------------
# _load_features_dataframe — real Hive-partitioned dir end-to-end
# ---------------------------------------------------------------------------


def test_load_features_dataframe_partitioned_dir_reads_only_recent_years(
    tmp_path: Path,
) -> None:
    year_dir_2013 = tmp_path / "race_year=2013"
    year_dir_2013.mkdir()
    _make_df().drop("race_year").write_parquet(year_dir_2013 / "part-0.parquet")
    year_dir_2014 = tmp_path / "race_year=2014"
    year_dir_2014.mkdir()
    _make_df().drop("race_year").write_parquet(year_dir_2014 / "part-0.parquet")
    year_dir_2015 = tmp_path / "race_year=2015"
    year_dir_2015.mkdir()
    _make_df().drop("race_year").write_parquet(year_dir_2015 / "part-0.parquet")
    df = subject._load_features_dataframe(tmp_path, "20150101")
    assert not df.is_empty()
    assert set(df["race_year"].unique().to_list()) == {2014, 2015}


# ---------------------------------------------------------------------------
# _deploy_cf_container / --cf-deploy
# ---------------------------------------------------------------------------


def test_cf_deploy_default_is_false() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        assert learner._cf_deploy is False


def test_cf_deploy_true_is_stored() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = subject.ContinuousLearner(
            registry=reg,
            df=_make_df(),
            category="jra",
            repo_root=Path("/fake/repo"),
            scripts_dir=Path("/fake/scripts"),
            cf_deploy=True,
        )
        assert learner._cf_deploy is True


def test_deploy_cf_container_runs_wrangler_deploy_in_container_dir() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = subject.ContinuousLearner(
            registry=reg,
            df=_make_df(),
            category="jra",
            repo_root=Path("/repo"),
            scripts_dir=Path("/fake/scripts"),
            cf_deploy=True,
        )
        with patch("subprocess.run") as mock_run:
            learner._deploy_cf_container()
        cmd = mock_run.call_args.args[0]
        assert cmd == ["bunx", "wrangler", "deploy"]
        assert mock_run.call_args.kwargs["cwd"] == "/repo/apps/finish-position-predict-container"
        assert mock_run.call_args.kwargs["check"] is True
        assert mock_run.call_args.kwargs["timeout"] == subject.DEFAULT_CF_DEPLOY_TIMEOUT_S


def test_deploy_calls_cf_container_when_cf_deploy_true() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = subject.ContinuousLearner(
            registry=reg,
            df=_make_df(),
            category="jra",
            repo_root=Path("/repo"),
            scripts_dir=Path("/fake/scripts"),
            cf_deploy=True,
        )
        entry = _make_entry(ndcg=0.75, feature_names=["feat_speed"])
        with (
            patch(
                "learning.continuous_learner.write_filtered_parquet",
                return_value=Path("/tmp/f.parquet"),
            ),
            patch.object(
                learner, "_train_production_model", return_value=Path("/tmp/model")
            ),
            patch.object(learner, "_stage_model", return_value=Path("/tmp/staged")),
            patch.object(learner, "_update_model_meta_json", return_value=None),
            patch.object(learner, "_deploy_cf_container") as mock_cf,
        ):
            learner._deploy(entry)
        mock_cf.assert_called_once()


def test_deploy_skips_cf_container_when_cf_deploy_false() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        entry = _make_entry(ndcg=0.75, feature_names=["feat_speed"])
        with (
            patch(
                "learning.continuous_learner.write_filtered_parquet",
                return_value=Path("/tmp/f.parquet"),
            ),
            patch.object(
                learner, "_train_production_model", return_value=Path("/tmp/model")
            ),
            patch.object(learner, "_stage_model", return_value=Path("/tmp/staged")),
            patch.object(learner, "_update_model_meta_json", return_value=None),
            patch.object(learner, "_rebuild_docker"),
            patch.object(learner, "_deploy_cf_container") as mock_cf,
        ):
            learner._deploy(entry)
        mock_cf.assert_not_called()


def test_main_forwards_cf_deploy_and_log_subgroup_flags(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().write_parquet(parquet_path)
    registry_path = tmp_path / "reg.duckdb"
    captured: dict[str, object] = {}

    with (
        patch.object(
            subject.ContinuousLearner,
            "__init__",
            _capture_learner_kwargs(captured),
        ),
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
                "--cf-deploy",
                "--log-subgroup",
                "--max-rounds",
                "1",
            ]
        )

    assert captured["cf_deploy"] is True
    assert captured["log_subgroup"] is True


def test_main_cf_deploy_and_log_subgroup_default_false(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().write_parquet(parquet_path)
    registry_path = tmp_path / "reg.duckdb"
    captured: dict[str, object] = {}

    with (
        patch.object(
            subject.ContinuousLearner,
            "__init__",
            _capture_learner_kwargs(captured),
        ),
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
            ]
        )

    assert captured["cf_deploy"] is False
    assert captured["log_subgroup"] is False


# ---------------------------------------------------------------------------
# _log_subgroup_diagnostics / _collect_active_predictions / --log-subgroup
# ---------------------------------------------------------------------------


def test_log_subgroup_default_is_false() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        assert learner._log_subgroup is False


def test_log_subgroup_true_is_stored() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = subject.ContinuousLearner(
            registry=reg,
            df=_make_df(),
            category="jra",
            repo_root=Path("/fake/repo"),
            scripts_dir=Path("/fake/scripts"),
            log_subgroup=True,
        )
        assert learner._log_subgroup is True


def test_run_calls_log_subgroup_when_enabled() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = subject.ContinuousLearner(
            registry=reg,
            df=_make_df(),
            category="jra",
            repo_root=Path("/fake/repo"),
            scripts_dir=Path("/fake/scripts"),
            log_subgroup=True,
        )
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy"),
            patch.object(learner, "_check_and_try_inverses"),
            patch.object(learner, "_analyze_feature_enrichment"),
            patch.object(learner, "_log_subgroup_diagnostics") as mock_log,
        ):
            learner.run(max_rounds=1)
        mock_log.assert_called_once_with()


def test_run_skips_log_subgroup_on_non_fifth_round() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = subject.ContinuousLearner(
            registry=reg,
            df=_make_df(),
            category="jra",
            repo_root=Path("/fake/repo"),
            scripts_dir=Path("/fake/scripts"),
            log_subgroup=True,
        )
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy"),
            patch.object(learner, "_check_and_try_inverses"),
            patch.object(learner, "_analyze_feature_enrichment"),
            patch.object(learner, "_log_subgroup_diagnostics") as mock_log,
        ):
            learner.run(max_rounds=5)
        assert mock_log.call_count == 1


def test_run_skips_log_subgroup_when_disabled() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy"),
            patch.object(learner, "_check_and_try_inverses"),
            patch.object(learner, "_analyze_feature_enrichment"),
            patch.object(learner, "_log_subgroup_diagnostics") as mock_log,
        ):
            learner.run(max_rounds=1)
        mock_log.assert_not_called()


def test_log_subgroup_diagnostics_skips_when_no_active_entry() -> None:
    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.get_active_entry.return_value = None
    learner = _make_learner(registry=mock_registry)
    with patch.object(learner, "_collect_active_predictions") as mock_collect:
        learner._log_subgroup_diagnostics()
    mock_collect.assert_not_called()


def test_log_subgroup_diagnostics_skips_when_no_predictions() -> None:
    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.get_active_entry.return_value = _make_entry(
        feature_names=["feat_speed"]
    )
    learner = _make_learner(registry=mock_registry)
    empty = pl.DataFrame(
        schema={
            "race_id": pl.Utf8,
            "ketto_toroku_bango": pl.Utf8,
            "predicted_rank": pl.Int64,
        }
    )
    with (
        patch.object(learner, "_collect_active_predictions", return_value=empty),
        patch(
            "learning.continuous_learner.compute_subgroup_diagnostics"
        ) as mock_compute,
    ):
        learner._log_subgroup_diagnostics()
    mock_compute.assert_not_called()


def test_log_subgroup_diagnostics_logs_each_subgroup(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.get_active_entry.return_value = _make_entry(
        feature_names=["feat_speed"]
    )
    learner = _make_learner(registry=mock_registry)
    preds = pl.DataFrame(
        {
            "race_id": ["r1"],
            "ketto_toroku_bango": ["horse_000"],
            "predicted_rank": [1],
        }
    )
    metrics = [
        {
            "subgroup": "jra_turf_mile_G2_summer_10",
            "category": "jra",
            "surface": "turf",
            "distance_band": "mile",
            "class_label": "G2",
            "season": "summer",
            "venue": "10",
            "race_count": 12,
            "ndcg_at_3": 0.61,
            "top1_accuracy": 0.5,
            "top3_box_accuracy": 0.25,
        }
    ]
    with (
        patch.object(learner, "_collect_active_predictions", return_value=preds),
        patch(
            "learning.continuous_learner.compute_subgroup_diagnostics",
            return_value=metrics,
        ),
    ):
        with caplog.at_level(logging.INFO, logger="learning.continuous_learner"):
            learner._log_subgroup_diagnostics()
    assert any("turf" in r.message for r in caplog.records)


def test_log_subgroup_diagnostics_handles_empty_metrics(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    mock_registry = MagicMock(spec=FeatureRegistry)
    mock_registry.get_active_entry.return_value = _make_entry(
        feature_names=["feat_speed"]
    )
    learner = _make_learner(registry=mock_registry)
    preds = pl.DataFrame(
        {
            "race_id": ["r1"],
            "ketto_toroku_bango": ["horse_000"],
            "predicted_rank": [1],
        }
    )
    with (
        patch.object(learner, "_collect_active_predictions", return_value=preds),
        patch(
            "learning.continuous_learner.compute_subgroup_diagnostics",
            return_value=[],
        ),
    ):
        with caplog.at_level(logging.INFO, logger="learning.continuous_learner"):
            learner._log_subgroup_diagnostics()
    assert any("no subgroups to report" in r.message for r in caplog.records)


def test_log_surface_summary_logs_turf_and_dirt_groups(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    from learning.subgroup_diagnostics import SubgroupMetrics

    learner = _make_learner()
    metrics = cast(
        "list[SubgroupMetrics]",
        [
            {
                "subgroup": "jra_turf_mile_G2_summer_10",
                "category": "jra",
                "surface": "turf",
                "distance_band": "mile",
                "class_label": "G2",
                "season": "summer",
                "venue": "10",
                "race_count": 10,
                "ndcg_at_3": 0.6,
                "top1_accuracy": 0.5,
                "top3_box_accuracy": 0.25,
            },
            {
                "subgroup": "nar_dirt_sprint_A_winter_40",
                "category": "nar",
                "surface": "dirt",
                "distance_band": "sprint",
                "class_label": "A",
                "season": "winter",
                "venue": "40",
                "race_count": 20,
                "ndcg_at_3": 0.4,
                "top1_accuracy": 0.3,
                "top3_box_accuracy": 0.15,
            },
        ],
    )
    with caplog.at_level(logging.INFO, logger="learning.continuous_learner"):
        learner._log_surface_summary(metrics)
    messages = [r.getMessage() for r in caplog.records]
    assert any("surface summary" in m for m in messages)
    assert any("surface=turf" in m for m in messages)
    assert any("surface=dirt" in m for m in messages)


def test_log_surface_summary_handles_empty_metrics(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    learner = _make_learner()
    with caplog.at_level(logging.INFO, logger="learning.continuous_learner"):
        learner._log_surface_summary([])
    messages = [r.getMessage() for r in caplog.records]
    assert any("surface summary" in m for m in messages)
    assert not any("surface=" in m for m in messages)


def test_log_surface_summary_weights_average_by_race_count(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    from learning.subgroup_diagnostics import SubgroupMetrics

    learner = _make_learner()
    metrics = cast(
        "list[SubgroupMetrics]",
        [
            {
                "subgroup": "jra_turf_mile_G2_summer_10",
                "category": "jra",
                "surface": "turf",
                "distance_band": "mile",
                "class_label": "G2",
                "season": "summer",
                "venue": "10",
                "race_count": 10,
                "ndcg_at_3": 0.6,
                "top1_accuracy": 0.6,
                "top3_box_accuracy": 0.6,
            },
            {
                "subgroup": "jra_turf_sprint_G1_winter_10",
                "category": "jra",
                "surface": "turf",
                "distance_band": "sprint",
                "class_label": "G1",
                "season": "winter",
                "venue": "10",
                "race_count": 30,
                "ndcg_at_3": 0.2,
                "top1_accuracy": 0.2,
                "top3_box_accuracy": 0.2,
            },
        ],
    )
    with caplog.at_level(logging.INFO, logger="learning.continuous_learner"):
        learner._log_surface_summary(metrics)
    turf_lines = [
        r.getMessage()
        for r in caplog.records
        if "surface=turf" in r.getMessage()
    ]
    assert turf_lines == [
        "│  surface=turf    races=   40  ndcg@3=0.3000  top1=0.3000  top3_box=0.3000"
    ]


def test_log_surface_summary_skips_zero_race_surface(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    from learning.subgroup_diagnostics import SubgroupMetrics

    learner = _make_learner()
    metrics = cast(
        "list[SubgroupMetrics]",
        [
            {
                "subgroup": "jra_turf_mile_G2_summer_10",
                "category": "jra",
                "surface": "turf",
                "distance_band": "mile",
                "class_label": "G2",
                "season": "summer",
                "venue": "10",
                "race_count": 0,
                "ndcg_at_3": 0.0,
                "top1_accuracy": 0.0,
                "top3_box_accuracy": 0.0,
            }
        ],
    )
    with caplog.at_level(logging.INFO, logger="learning.continuous_learner"):
        learner._log_surface_summary(metrics)
    messages = [r.getMessage() for r in caplog.records]
    assert any("surface summary" in m for m in messages)
    assert not any("surface=" in m for m in messages)


def _make_df_3years() -> pl.DataFrame:
    rows = []
    for year in [2022, 2023, 2024]:
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
    return pl.DataFrame(rows)


def test_collect_active_predictions_stacks_fold_predictions() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg,
            df=_make_df_3years(),
            validation_years=[2023, 2024],
            train_start="20220101",
        )
        preds = pl.DataFrame(
            {
                "race_id": ["r1", "r1"],
                "ketto_toroku_bango": ["a", "b"],
                "predicted_rank": [1, 2],
                "finish_position": [1, 2],
            }
        )
        with patch(
            "learning.continuous_learner.predict_fold_with_backend",
            return_value=preds,
        ) as mock_predict:
            result = learner._collect_active_predictions(["feat_speed"])
        assert list(result.columns) == ["race_id", "ketto_toroku_bango", "predicted_rank"]
        assert len(result) == 12
        assert mock_predict.call_count == 6


def test_collect_active_predictions_returns_slim_projection_of_wide_preds() -> None:
    # The result is the slim 3-column projection of the wide prediction frame, so
    # the full-width preds (with finish_position) can be released each iteration.
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg,
            df=_make_df_3years(),
            validation_years=[2023],
            train_start="20220101",
        )
        wide = pl.DataFrame(
            {
                "race_id": ["r1", "r1"],
                "ketto_toroku_bango": ["a", "b"],
                "predicted_rank": [1, 2],
                "finish_position": [1, 2],
            }
        )
        with patch(
            "learning.continuous_learner.predict_fold_with_backend",
            return_value=wide,
        ):
            result = learner._collect_active_predictions(["feat_speed"])
        assert result.columns == ["race_id", "ketto_toroku_bango", "predicted_rank"]
        assert "finish_position" not in result.columns
        assert set(result["predicted_rank"].to_list()) == {1, 2}


def test_collect_active_predictions_skips_none_predictions() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg,
            df=_make_df_3years(),
            validation_years=[2023],
            train_start="20220101",
        )
        with patch(
            "learning.continuous_learner.predict_fold_with_backend",
            return_value=None,
        ):
            result = learner._collect_active_predictions(["feat_speed"])
        assert result.is_empty()
        assert list(result.columns) == ["race_id", "ketto_toroku_bango", "predicted_rank"]


def test_collect_active_predictions_skips_empty_folds() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg, validation_years=[1990], train_start="19890101"
        )
        with patch(
            "learning.continuous_learner.predict_fold_with_backend"
        ) as mock_predict:
            result = learner._collect_active_predictions(["feat_speed"])
        assert result.is_empty()
        mock_predict.assert_not_called()


# ---------------------------------------------------------------------------
# _get_folds (fold-split caching)
# ---------------------------------------------------------------------------


def test_fold_cache_starts_empty() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        assert learner._fold_cache == {}


def test_get_folds_cache_miss_computes_via_split_walk_forward() -> None:
    fold_2023 = {"train_df": None, "valid_df": None, "valid_year": 2023}
    fold_2024 = {"train_df": None, "valid_df": None, "valid_year": 2024}
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with patch(
            "learning.continuous_learner.split_walk_forward",
            side_effect=[fold_2023, fold_2024],
        ) as mock_split:
            folds = learner._get_folds([2023, 2024])
        assert mock_split.call_count == 2
        assert folds == [fold_2023, fold_2024]


def test_get_folds_cache_hit_does_not_recompute() -> None:
    fold_2023 = {"train_df": None, "valid_df": None, "valid_year": 2023}
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with patch(
            "learning.continuous_learner.split_walk_forward",
            return_value=fold_2023,
        ) as mock_split:
            first = learner._get_folds([2023])
            second = learner._get_folds([2023])
        assert mock_split.call_count == 1
        assert first[0] is second[0]


def test_get_folds_partial_cache_only_computes_uncached_years() -> None:
    fold_2023 = {"train_df": None, "valid_df": None, "valid_year": 2023}
    fold_2024 = {"train_df": None, "valid_df": None, "valid_year": 2024}
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with patch(
            "learning.continuous_learner.split_walk_forward",
            return_value=fold_2023,
        ):
            learner._get_folds([2023])
        with patch(
            "learning.continuous_learner.split_walk_forward",
            return_value=fold_2024,
        ) as mock_split_second:
            folds = learner._get_folds([2023, 2024])
        assert mock_split_second.call_count == 1
        assert folds[0] is fold_2023
        assert folds[1] is fold_2024


def test_get_folds_preserves_input_order() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with patch(
            "learning.continuous_learner.split_walk_forward",
            side_effect=lambda df, ts, yr: {
                "train_df": None,
                "valid_df": None,
                "valid_year": yr,
            },
        ):
            folds = learner._get_folds([2024, 2023])
        assert [fold["valid_year"] for fold in folds] == [2024, 2023]


def test_collect_active_predictions_uses_cached_folds_across_two_calls() -> None:
    preds = pl.DataFrame(
        {
            "race_id": ["r1", "r1"],
            "ketto_toroku_bango": ["a", "b"],
            "predicted_rank": [1, 2],
            "finish_position": [1, 2],
        }
    )
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(
            registry=reg,
            df=_make_df_3years(),
            validation_years=[2023, 2024],
            train_start="20220101",
        )
        with (
            patch(
                "learning.continuous_learner.predict_fold_with_backend",
                return_value=preds,
            ),
            patch(
                "learning.continuous_learner.split_walk_forward",
                wraps=split_walk_forward,
            ) as mock_split,
        ):
            learner._collect_active_predictions(["feat_speed"])
            learner._collect_active_predictions(["feat_speed"])
        assert mock_split.call_count == 2


# ---------------------------------------------------------------------------
# _auto_tune_resources / --auto-tune / --no-auto-tune
# ---------------------------------------------------------------------------


def test_auto_tune_nthread_constants() -> None:
    assert subject._MIN_NTHREAD == 2
    assert subject._MAX_NTHREAD == 6
    assert subject._MIN_FREE_MEM_GB == 8.0


def test_auto_tune_resources_returns_min_when_load_high() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        fake = MagicMock()
        fake.cpu_count.return_value = 8
        fake.getloadavg.return_value = (8.0, 0, 0)
        fake.virtual_memory.return_value = MagicMock(available=32 * 1024**3)
        with patch.object(subject, "_psutil", fake):
            result = learner._auto_tune_resources()
        assert result == subject._MIN_NTHREAD


def test_auto_tune_resources_returns_above_min_when_load_low() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        fake = MagicMock()
        fake.cpu_count.return_value = 15
        fake.getloadavg.return_value = (1.0, 0, 0)
        fake.virtual_memory.return_value = MagicMock(available=40 * 1024**3)
        with patch.object(subject, "_psutil", fake):
            result = learner._auto_tune_resources()
        assert subject._MIN_NTHREAD < result <= subject._MAX_NTHREAD


def test_auto_tune_resources_returns_min_when_free_mem_low() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        fake = MagicMock()
        fake.cpu_count.return_value = 15
        fake.getloadavg.return_value = (1.0, 0, 0)
        fake.virtual_memory.return_value = MagicMock(available=4 * 1024**3)
        with patch.object(subject, "_psutil", fake):
            result = learner._auto_tune_resources()
        assert result == subject._MIN_NTHREAD


def test_auto_tune_resources_returns_max_when_psutil_missing() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        with patch.object(subject, "_psutil", None):
            result = learner._auto_tune_resources()
        assert result == subject._MAX_NTHREAD


def test_auto_tune_resources_clamps_idle_fraction_to_zero_when_overloaded() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        fake = MagicMock()
        fake.cpu_count.return_value = 4
        fake.getloadavg.return_value = (10.0, 0, 0)
        fake.virtual_memory.return_value = MagicMock(available=32 * 1024**3)
        with patch.object(subject, "_psutil", fake):
            result = learner._auto_tune_resources()
        assert result == subject._MIN_NTHREAD


def test_auto_tune_resources_uses_eight_when_cpu_count_none() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        fake = MagicMock()
        fake.cpu_count.return_value = None
        fake.getloadavg.return_value = (1.0, 0, 0)
        fake.virtual_memory.return_value = MagicMock(available=32 * 1024**3)
        with patch.object(subject, "_psutil", fake):
            result = learner._auto_tune_resources()
        assert subject._MIN_NTHREAD <= result <= subject._MAX_NTHREAD


def test_run_does_not_auto_tune_when_disabled() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, auto_tune=False)
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy"),
            patch.object(learner, "_auto_tune_resources") as mock_tune,
        ):
            learner.run(max_rounds=1)
        mock_tune.assert_not_called()


def test_run_auto_tunes_once_per_round_when_enabled() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, auto_tune=True)
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy"),
            patch.object(
                learner, "_auto_tune_resources", return_value=4
            ) as mock_tune,
        ):
            learner.run(max_rounds=1)
        mock_tune.assert_called_once()


def test_auto_tune_default_is_true() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        assert learner._auto_tune is True


def test_auto_tune_false_is_stored() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, auto_tune=False)
        assert learner._auto_tune is False


def test_main_no_auto_tune_flag_disables_auto_tune(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().write_parquet(parquet_path)
    registry_path = tmp_path / "reg.duckdb"

    with (
        patch.object(subject.ContinuousLearner, "_explore_round"),
        patch.object(subject.ContinuousLearner, "_maybe_deploy"),
        patch.object(subject.ContinuousLearner, "_auto_tune_resources") as mock_tune,
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
                "--no-auto-tune",
                "--max-rounds",
                "1",
            ]
        )

    mock_tune.assert_not_called()


def test_main_default_enables_auto_tune(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().write_parquet(parquet_path)
    registry_path = tmp_path / "reg.duckdb"

    with (
        patch.object(subject.ContinuousLearner, "_explore_round"),
        patch.object(subject.ContinuousLearner, "_maybe_deploy"),
        patch.object(
            subject.ContinuousLearner, "_auto_tune_resources", return_value=4
        ) as mock_tune,
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
            ]
        )

    assert mock_tune.call_count >= 1


# ---------------------------------------------------------------------------
# feature_explorer.predict_fold_with_backend / select_fold_features
# ---------------------------------------------------------------------------


def _make_explorer_fold() -> object:
    rows = []
    for i in range(4):
        rows.append(
            {
                "source": "jra",
                "race_date": "20220601",
                "kaisai_nen": "2022",
                "kaisai_tsukihi": "0601",
                "keibajo_code": "10",
                "race_bango": "01",
                "ketto_toroku_bango": f"horse_{i:03d}",
                "umaban": i + 1,
                "category": "jra",
                "race_id": "2022_race_01",
                "race_year": 2022,
                "feature_schema_version": "1",
                "finish_position": i + 1,
                "finish_norm": 0.5,
                "target_corner_1_norm": 0.5,
                "target_corner_3_norm": 0.5,
                "target_corner_4_norm": 0.5,
                "target_running_style_class": 0,
                "feat_speed": float(i),
                "feat_jockey": 0.3,
            }
        )
    fold_df = pl.DataFrame(rows)
    return {"train_df": fold_df, "valid_df": fold_df.clone(), "valid_year": 2023}


def _make_explorer_meta_only_fold() -> object:
    rows = []
    for i in range(4):
        rows.append(
            {
                "source": "jra",
                "race_date": "20220601",
                "kaisai_nen": "2022",
                "kaisai_tsukihi": "0601",
                "keibajo_code": "10",
                "race_bango": "01",
                "ketto_toroku_bango": f"horse_{i:03d}",
                "umaban": i + 1,
                "category": "jra",
                "race_id": "2022_race_01",
                "race_year": 2022,
                "feature_schema_version": "1",
                "finish_position": i + 1,
                "finish_norm": 0.5,
                "target_corner_1_norm": 0.5,
                "target_corner_3_norm": 0.5,
                "target_corner_4_norm": 0.5,
                "target_running_style_class": 0,
            }
        )
    fold_df = pl.DataFrame(rows)
    return {"train_df": fold_df, "valid_df": fold_df.clone(), "valid_year": 2023}


def test_predict_fold_with_backend_lightgbm_returns_predictions_with_finish_position() -> None:
    import learning.feature_explorer as explorer
    from finish_position_lightgbm import FoldSplit

    fold = cast("FoldSplit", _make_explorer_fold())
    preds_df = pl.DataFrame(
        {
            "race_id": ["2022_race_01", "2022_race_01", "2022_race_01", "2022_race_01"],
            "ketto_toroku_bango": ["horse_000", "horse_001", "horse_002", "horse_003"],
            "predicted_rank": [1, 2, 3, 4],
        }
    )
    with patch(
        "learning.feature_explorer.run_walk_forward_fold",
        return_value=(MagicMock(), preds_df, {"ndcg_at_3": 0.8}),
    ) as mock_fold:
        result = explorer.predict_fold_with_backend(
            fold, "lightgbm", explorer.DEFAULT_PARAMS
        )
    assert result is not None
    assert "predicted_rank" in result.columns
    assert "finish_position" in result.columns
    mock_fold.assert_called_once()


def test_predict_fold_with_backend_xgboost_returns_valid_predictions() -> None:
    import learning.feature_explorer as explorer
    from finish_position_lightgbm import FoldSplit

    fold = cast("FoldSplit", _make_explorer_fold())
    valid_preds = pl.DataFrame(
        {
            "race_id": ["r1", "r1", "r1", "r1"],
            "ketto_toroku_bango": ["a", "b", "c", "d"],
            "predicted_rank": [1, 2, 3, 4],
            "finish_position": [1, 2, 3, 4],
        }
    )
    with patch(
        "learning.feature_explorer.train_xgboost_ranker",
        return_value=(MagicMock(), {"valid_predictions": valid_preds}),
    ) as mock_xgb:
        result = explorer.predict_fold_with_backend(
            fold, "xgboost", explorer.DEFAULT_PARAMS
        )
    assert result is valid_preds
    mock_xgb.assert_called_once()


def test_predict_fold_with_backend_catboost_returns_valid_predictions() -> None:
    import learning.feature_explorer as explorer
    from finish_position_lightgbm import FoldSplit

    fold = cast("FoldSplit", _make_explorer_fold())
    valid_preds = pl.DataFrame(
        {
            "race_id": ["r1", "r1", "r1", "r1"],
            "ketto_toroku_bango": ["a", "b", "c", "d"],
            "predicted_rank": [1, 2, 3, 4],
            "finish_position": [1, 2, 3, 4],
        }
    )
    with patch(
        "learning.feature_explorer.train_catboost_ranker",
        return_value={"valid_predictions": valid_preds},
    ) as mock_cb:
        result = explorer.predict_fold_with_backend(
            fold, "catboost", explorer.DEFAULT_PARAMS
        )
    assert result is valid_preds
    mock_cb.assert_called_once()


def test_predict_fold_with_backend_xgboost_returns_none_when_no_numeric_features() -> None:
    import learning.feature_explorer as explorer
    from finish_position_lightgbm import FoldSplit

    fold = cast("FoldSplit", _make_explorer_meta_only_fold())
    result = explorer.predict_fold_with_backend(
        fold, "xgboost", explorer.DEFAULT_PARAMS
    )
    assert result is None


def test_predict_fold_with_backend_catboost_returns_none_when_no_feature_cols() -> None:
    import learning.feature_explorer as explorer
    from finish_position_lightgbm import FoldSplit

    fold = cast("FoldSplit", _make_explorer_meta_only_fold())
    result = explorer.predict_fold_with_backend(
        fold, "catboost", explorer.DEFAULT_PARAMS
    )
    assert result is None


def test_select_fold_features_public_alias_keeps_requested_columns() -> None:
    import learning.feature_explorer as explorer
    from finish_position_lightgbm import FoldSplit

    fold = cast("FoldSplit", _make_explorer_fold())
    result = explorer.select_fold_features(fold, {"feat_speed"})
    assert "feat_speed" in result["train_df"].columns
    assert "feat_jockey" not in result["train_df"].columns


def test_select_fold_features_public_alias_preserves_valid_year() -> None:
    import learning.feature_explorer as explorer
    from finish_position_lightgbm import FoldSplit

    fold = cast("FoldSplit", _make_explorer_fold())
    result = explorer.select_fold_features(fold, {"feat_speed"})
    assert result["valid_year"] == 2023


# ---------------------------------------------------------------------------
# cf_deploy_dir — configurable wrangler deploy directory
# ---------------------------------------------------------------------------


def test_cf_deploy_dir_default_is_none() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = subject.ContinuousLearner(
            registry=reg,
            df=_make_df(),
            category="jra",
            repo_root=Path("/repo"),
            scripts_dir=Path("/fake/scripts"),
        )
        assert learner._cf_deploy_dir is None


def test_cf_deploy_dir_is_stored_when_provided() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = subject.ContinuousLearner(
            registry=reg,
            df=_make_df(),
            category="jra",
            repo_root=Path("/repo"),
            scripts_dir=Path("/fake/scripts"),
            cf_deploy=True,
            cf_deploy_dir=Path("/repo/apps/finish-position-cron"),
        )
        assert learner._cf_deploy_dir == Path("/repo/apps/finish-position-cron")


def test_deploy_cf_container_uses_cf_deploy_dir_when_set() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = subject.ContinuousLearner(
            registry=reg,
            df=_make_df(),
            category="jra",
            repo_root=Path("/repo"),
            scripts_dir=Path("/fake/scripts"),
            cf_deploy=True,
            cf_deploy_dir=Path("/repo/apps/finish-position-cron"),
        )
        with patch("subprocess.run") as mock_run:
            learner._deploy_cf_container()
        cmd = mock_run.call_args.args[0]
        assert cmd == ["bunx", "wrangler", "deploy"]
        assert mock_run.call_args.kwargs["cwd"] == "/repo/apps/finish-position-cron"
        assert mock_run.call_args.kwargs["check"] is True
        assert mock_run.call_args.kwargs["timeout"] == subject.DEFAULT_CF_DEPLOY_TIMEOUT_S


def test_deploy_cf_container_falls_back_to_container_app_dir_when_dir_none() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = subject.ContinuousLearner(
            registry=reg,
            df=_make_df(),
            category="jra",
            repo_root=Path("/repo"),
            scripts_dir=Path("/fake/scripts"),
            cf_deploy=True,
            cf_deploy_dir=None,
        )
        with patch("subprocess.run") as mock_run:
            learner._deploy_cf_container()
        assert (
            mock_run.call_args.kwargs["cwd"]
            == "/repo/apps/finish-position-predict-container"
        )


def test_main_forwards_cf_deploy_dir(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().write_parquet(parquet_path)
    registry_path = tmp_path / "reg.duckdb"
    deploy_dir = tmp_path / "apps" / "finish-position-cron"
    captured: dict[str, object] = {}

    with (
        patch.object(
            subject.ContinuousLearner,
            "__init__",
            _capture_learner_kwargs(captured),
        ),
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
                "--cf-deploy",
                "--cf-deploy-dir",
                str(deploy_dir),
                "--max-rounds",
                "1",
            ]
        )

    assert captured["cf_deploy_dir"] == deploy_dir


def test_main_cf_deploy_dir_default_is_none(tmp_path: Path) -> None:
    parquet_path = tmp_path / "features.parquet"
    _make_df().write_parquet(parquet_path)
    registry_path = tmp_path / "reg.duckdb"
    captured: dict[str, object] = {}

    with (
        patch.object(
            subject.ContinuousLearner,
            "__init__",
            _capture_learner_kwargs(captured),
        ),
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
            ]
        )

    assert captured["cf_deploy_dir"] is None


# ---------------------------------------------------------------------------
# screening=True — blind holdout does NOT use screening
# ---------------------------------------------------------------------------


def test_evaluate_blind_holdout_does_not_use_screening() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg, blind_holdout_year=2025)
        entry = _make_entry(feature_names=["feat_speed"])
        with patch(
            "learning.continuous_learner.evaluate_feature_set",
            return_value=0.5,
        ) as mock_eval:
            learner._evaluate_blind_holdout(entry)
        # evaluate_feature_set has no screening param -- it always uses full params.
        # Just verify it was called without any screening kwarg.
        assert "screening" not in mock_eval.call_args.kwargs


# ---------------------------------------------------------------------------
# subgroup diagnostics skipped on non-5th rounds
# ---------------------------------------------------------------------------


def test_run_calls_subgroup_diagnostics_on_round_5() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = subject.ContinuousLearner(
            registry=reg,
            df=_make_df(),
            category="jra",
            repo_root=Path("/fake/repo"),
            scripts_dir=Path("/fake/scripts"),
            log_subgroup=True,
        )
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy"),
            patch.object(learner, "_check_and_try_inverses"),
            patch.object(learner, "_analyze_feature_enrichment"),
            patch.object(learner, "_log_subgroup_diagnostics") as mock_log,
        ):
            learner.run(max_rounds=6)
        # Rounds 0, 1, 2, 3, 4, 5 — rounds 0 and 5 have round_num % 5 == 0.
        assert mock_log.call_count == 2


def test_run_calls_subgroup_diagnostics_on_round_10() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = subject.ContinuousLearner(
            registry=reg,
            df=_make_df(),
            category="jra",
            repo_root=Path("/fake/repo"),
            scripts_dir=Path("/fake/scripts"),
            log_subgroup=True,
        )
        with (
            patch.object(learner, "_explore_round"),
            patch.object(learner, "_maybe_deploy"),
            patch.object(learner, "_check_and_try_inverses"),
            patch.object(learner, "_analyze_feature_enrichment"),
            patch.object(learner, "_log_subgroup_diagnostics") as mock_log,
        ):
            learner.run(max_rounds=11)
        # Rounds 0..10 — rounds 0, 5, 10 have round_num % 5 == 0.
        assert mock_log.call_count == 3


# ---------------------------------------------------------------------------
# enrichment caching (_last_enrichment)
# ---------------------------------------------------------------------------


def test_last_enrichment_initialized_to_none() -> None:
    with FeatureRegistry(Path(":memory:")) as reg:
        learner = _make_learner(registry=reg)
        assert learner._last_enrichment is None
