from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Literal

import numpy as np
import polars as pl
import pytest
from catboost import CatBoostRanker
from learning.cell_training_scope import CellScopeConfig
from learning.history_ablation import (
    AblationConfig,
    exact_rank_metrics,
    main,
    parse_ablation_args,
    predict_ablation,
    run_ablation_experiment,
    train_ablation_fold,
)


@pytest.fixture
def training() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "race_id": ["a", "a", "b", "b"],
            "venue": ["54", "54", "54", "54"],
            "race_date": ["20230101", "20230101", "20230102", "20230102"],
            "horse_id": ["h1", "h2", "h1", "h2"],
            "horse_number": [1, 2, 1, 2],
            "finish": [1, 2, 2, 1],
            "distance_m": [1200.0, 1200.0, 1400.0, 1400.0],
            "field_size": [2, 2, 2, 2],
            "age": [3, 4, 3, 4],
            "track_code": [24, 24, 24, 24],
            "sex_code": [1, 2, 1, 2],
            "going": [1, 1, 1, 1],
            "month": [1, 1, 1, 1],
            "past_runs": [1, 2, 3, 4],
            "past_finish_mean": [0.2, 0.4, 0.5, 0.1],
            "past_win_rate": [0.5, 0.4, 0.3, 0.6],
            "days_since": [14, 15, 1, 1],
            "jockey_past_win_rate": [0.1, 0.2, 0.1, 0.2],
            "trainer_past_win_rate": [0.1, 0.2, 0.1, 0.2],
            "past_speed_mean": [15.0, 14.0, 14.0, 15.0],
            "past_speed_365d": [15.0, 14.0, 14.0, 15.0],
            "clock_seconds": [1.0, 9999.0, 9999.0, 1.0],
        }
    )


@pytest.mark.parametrize("include_speed", [False, True])
def test_tiny_training_uses_only_prerace_allowlist(
    training: pl.DataFrame, include_speed: bool
) -> None:
    evaluation = training.with_columns(pl.lit("20240101").alias("race_date"))
    result = train_ablation_fold(
        training,
        evaluation,
        config=AblationConfig(include_speed=include_speed, iterations=2, threads=1),
    )
    assert result.predictions.height == 4
    assert "clock_seconds" not in result.features
    assert "finish" not in result.features
    assert set(result.metrics) == {"rank1", "rank2", "rank3", "rank4", "rank5"}


def test_rejects_empty_split(training: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="rows are required"):
        train_ablation_fold(
            training.head(0), training, config=AblationConfig(include_speed=True)
        )


def test_rejects_non_string_dates(training: pl.DataFrame) -> None:
    with pytest.raises(TypeError, match="YYYYMMDD strings"):
        train_ablation_fold(
            training.with_columns(pl.lit(None).alias("race_date")),
            training,
            config=AblationConfig(include_speed=True),
        )


def test_rejects_temporal_overlap(training: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="strictly precede"):
        train_ablation_fold(
            training, training, config=AblationConfig(include_speed=True)
        )


def test_rejects_missing_labels(training: pl.DataFrame) -> None:
    evaluation = training.with_columns(
        pl.lit("20240101").alias("race_date"),
        pl.lit(None).cast(pl.Int64).alias("finish"),
    )
    with pytest.raises(ValueError, match="observed finish labels"):
        train_ablation_fold(
            training, evaluation, config=AblationConfig(include_speed=True)
        )


def test_exact_ranks_use_race_denominator() -> None:
    predictions = pl.DataFrame(
        {
            "race_id": ["a", "a", "b", "b"],
            "finish": [1, 2, 2, 1],
            "predicted_rank": [1, 2, 1, 2],
        }
    )
    assert exact_rank_metrics(predictions) == {
        "rank1": 0.5,
        "rank2": 0.5,
        "rank3": 0.0,
        "rank4": 0.0,
        "rank5": 0.0,
    }


def test_cli_parses_explicit_trial() -> None:
    args = parse_ablation_args(
        [
            "--features",
            "features.parquet",
            "--output",
            "results",
            "--venue",
            "54",
            "--year",
            "2025",
            "--include-speed",
            "--iterations",
            "10",
            "--threads",
            "1",
        ]
    )
    assert args.venue == "54"
    assert args.year == 2025
    assert args.include_speed is True
    assert args.iterations == 10


@pytest.mark.parametrize("include_speed", [False, True])
@pytest.mark.parametrize("objective", ["winner", "top5"])
@pytest.mark.parametrize("relative_speed", [False, True])
def test_experiment_persists_scope_and_nonpromotion_provenance(
    training: pl.DataFrame,
    tmp_path: Path,
    include_speed: bool,
    objective: Literal["winner", "top5"],
    relative_speed: bool,
) -> None:
    evaluation = training.with_columns(
        pl.lit("20240101").alias("race_date"),
        (pl.col("race_id") + "eval").alias("race_id"),
    )
    history = pl.concat([training, evaluation]).with_columns(
        pl.lit("nar").alias("category")
    )
    history = history.with_columns(
        pl.col("race_date").str.strptime(pl.Date, "%Y%m%d").alias("observed_date"),
        (pl.col("horse_number") * 20.0 + 50).alias("clock_seconds"),
    )
    history.write_parquet(tmp_path / "features.parquet")
    report = run_ablation_experiment(
        tmp_path / "features.parquet",
        tmp_path / "results",
        scope_config=CellScopeConfig(
            cell_id="nar-54",
            category="nar",
            dimensions=(("venue", "54"),),
            training_cutoff=date(2024, 1, 1),
            evaluation_end=date(2024, 12, 31),
        ),
        config=AblationConfig(
            include_speed=include_speed,
            iterations=2,
            threads=1,
            objective=objective,
            include_relative_speed=relative_speed,
        ),
    )
    assert report["training_rows"] == 4
    assert report["evaluation_rows"] == 4
    assert report["promotion_eligible"] is False
    saved = json.loads(
        (tmp_path / "results" / "report.json").read_text(encoding="utf-8")
    )
    assert saved["source"] == "local-pg"
    assert (tmp_path / "results" / "model.cbm").is_file()
    assert pl.read_parquet(tmp_path / "results" / "seed-races.parquet").height == 2


@pytest.mark.parametrize("venue", ["54", "83"])
def test_main_passes_scope_and_fixed_budget_to_runner(
    monkeypatch: pytest.MonkeyPatch,
    venue: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def run_fake(
        feature_path: Path,
        output_dir: Path,
        *,
        scope_config: CellScopeConfig,
        config: AblationConfig,
    ) -> dict[str, object]:
        assert feature_path.name == "features.parquet"
        assert output_dir.name == "results"
        assert scope_config.training_cutoff == date(2026, 1, 1)
        assert scope_config.evaluation_end == date(2026, 9, 11)
        assert config.iterations == 200
        return {"promotion_eligible": False}

    monkeypatch.setattr("learning.history_ablation.run_ablation_experiment", run_fake)
    main(
        [
            "--features",
            "features.parquet",
            "--output",
            "results",
            "--venue",
            venue,
            "--year",
            "2026",
        ]
    )
    assert capsys.readouterr().out == '{"promotion_eligible": false}\n'


def test_prediction_rejects_wrong_feature_order(training: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="feature order"):
        predict_ablation(CatBoostRanker(), training, features=("venue",))


def test_prediction_rejects_empty_labels(training: pl.DataFrame) -> None:
    evaluation = training.with_columns(pl.lit("20240101").alias("race_date"))
    fitted = train_ablation_fold(
        training, evaluation, config=AblationConfig(include_speed=False, iterations=2)
    )
    with pytest.raises(ValueError, match="observed finish"):
        predict_ablation(fitted.model, evaluation.head(0), features=fitted.features)


@pytest.mark.parametrize("wrong_shape", [False, True])
def test_prediction_rejects_nonfinite_or_wrong_shape(
    training: pl.DataFrame,
    monkeypatch: pytest.MonkeyPatch,
    wrong_shape: bool,
) -> None:
    evaluation = training.with_columns(pl.lit("20240101").alias("race_date"))
    fitted = train_ablation_fold(
        training, evaluation, config=AblationConfig(include_speed=False, iterations=2)
    )
    scores = np.array([0.0]) if wrong_shape else np.full(4, np.nan)
    monkeypatch.setattr(fitted.model, "predict", lambda *_args, **_kwargs: scores)
    with pytest.raises(ValueError, match="finite research score"):
        predict_ablation(fitted.model, evaluation, features=fitted.features)


def test_unfitted_catboost_tree_count_is_optional() -> None:
    assert CatBoostRanker().tree_count_ is None


def test_empty_metrics_fail_closed() -> None:
    with pytest.raises(ValueError, match="empty race set"):
        exact_rank_metrics(pl.DataFrame({"race_id": []}))
