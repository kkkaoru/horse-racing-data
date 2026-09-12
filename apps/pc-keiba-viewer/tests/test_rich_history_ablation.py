from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import replace
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from learning.cell_training_scope import CellScopeConfig
from learning.rich_history_ablation import (
    RichConfig,
    main,
    prepare_rich_rows,
    rich_feature_names,
    rich_matrix,
    train_rich_fold,
)


@pytest.fixture
def script_sandbox(tmp_path: Path) -> Path:
    scripts = tmp_path / "repo" / "apps" / "viewer" / "scripts"
    scripts.mkdir(parents=True)
    uv = scripts / "uv"
    uv.write_text("#!/bin/sh\necho TRAINING_REQUESTED\nexit 73\n", encoding="utf-8")
    uv.chmod(0o755)
    return scripts


@pytest.mark.parametrize(
    "script_name", ["run-rich-body-ablation.sh", "run-rich-history-ablation.sh"]
)
@pytest.mark.parametrize("timestamp", ["202609120848", "202609120849"])
def test_rich_scripts_allow_training_until_cutoff(
    script_sandbox: Path, script_name: str, timestamp: str
) -> None:
    script = script_sandbox / script_name
    shutil.copyfile(Path(__file__).parents[1] / "scripts" / script_name, script)
    clock = script_sandbox / "date"
    clock.write_text(f"#!/bin/sh\necho {timestamp}\n", encoding="utf-8")
    clock.chmod(0o755)
    result = subprocess.run(
        ["bash", str(script)],
        env={
            **os.environ,
            "PATH": f"{script_sandbox}:/usr/bin:/bin",
            "RICH_YEARS": "2026",
        },
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    assert result.returncode == 73
    assert "TRAINING_REQUESTED" in result.stdout


@pytest.mark.parametrize(
    "script_name", ["run-rich-body-ablation.sh", "run-rich-history-ablation.sh"]
)
def test_rich_scripts_stop_after_cutoff(script_sandbox: Path, script_name: str) -> None:
    script = script_sandbox / script_name
    shutil.copyfile(Path(__file__).parents[1] / "scripts" / script_name, script)
    clock = script_sandbox / "date"
    clock.write_text("#!/bin/sh\necho 202609120850\n", encoding="utf-8")
    clock.chmod(0o755)
    result = subprocess.run(
        ["bash", str(script)],
        env={
            **os.environ,
            "PATH": f"{script_sandbox}:/usr/bin:/bin",
            "RICH_YEARS": "2026",
        },
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    assert result.returncode == 0
    assert "TRAINING_REQUESTED" not in result.stdout


@pytest.fixture
def scope_config() -> CellScopeConfig:
    return CellScopeConfig(
        cell_id="ban-ei-83",
        category="ban-ei",
        dimensions=(("venue", "83"),),
        training_cutoff=date(2024, 1, 1),
        evaluation_end=date(2024, 12, 31),
    )


@pytest.fixture
def history() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "race_id": ["nar-20220101-81-01"] * 2
            + ["nar-20230101-83-01"] * 2
            + ["nar-20240101-83-01"] * 2,
            "race_date": ["20220101"] * 2 + ["20230101"] * 2 + ["20240101"] * 2,
            "horse_id": ["a", "b", "a", "c", "a", "b"],
            "horse_number": [1, 2, 1, 2, 1, 2],
            "category": ["nar", "nar", "ban-ei", "ban-ei", "ban-ei", "ban-ei"],
            "venue": ["81", "81", "83", "83", "83", "83"],
            "finish": [1, 2, 1, 2, 1, 2],
        }
    ).with_columns(
        pl.lit(200.0).alias("distance_m"),
        pl.lit(2).alias("field_size"),
        pl.lit(4).alias("age"),
        pl.lit(0).alias("track_code"),
        pl.lit(1).alias("sex_code"),
        pl.lit(2).alias("going"),
        pl.lit(1).alias("month"),
        pl.lit(1).alias("past_runs"),
        pl.lit(0.5).alias("past_finish_mean"),
        pl.lit(0.5).alias("past_win_rate"),
        pl.lit(7).alias("days_since"),
        pl.lit(0.1).alias("jockey_past_win_rate"),
        pl.lit(0.2).alias("trainer_past_win_rate"),
        pl.lit(1.2).alias("past_speed_mean"),
        pl.lit(1.1).alias("past_speed_365d"),
        pl.lit(0.1).alias("past_relative_speed_mean"),
        pl.lit(0.2).alias("past_relative_speed_365d"),
        pl.lit(0.3).alias("past_relative_speed_28d"),
        pl.lit(100.0).alias("clock_seconds"),
        pl.col("race_date").str.strptime(pl.Date, "%Y%m%d").alias("observed_date"),
    )


@pytest.fixture
def native(history: pl.DataFrame) -> pl.DataFrame:
    return history.select(
        "race_id",
        "horse_id",
        pl.col("finish").alias("native_finish"),
        pl.when(pl.col("venue") == "81")
        .then(pl.lit("retired-base"))
        .otherwise(pl.lit("full"))
        .alias("native_context"),
        (3 - pl.col("horse_number")).alias("native_signal"),
    )


def test_scope_preserves_retired_competitors(
    history: pl.DataFrame, native: pl.DataFrame, scope_config: CellScopeConfig
) -> None:
    rows = prepare_rich_rows(
        history, native, scope_config=scope_config, native_features=("native_signal",)
    )
    assert rows.audit == {
        "required_rows": 6,
        "retained_rows": 6,
        "missing_native_unlabelled": 0,
        "retired_base_rows": 2,
        "training_rows": 4,
        "evaluation_rows": 2,
    }
    assert rows.training["horse_id"].to_list() == ["a", "b", "a", "c"]


@pytest.mark.parametrize("names", [(), ("native_signal", "native_signal")])
def test_bad_names_rejected(
    history: pl.DataFrame,
    native: pl.DataFrame,
    scope_config: CellScopeConfig,
    names: tuple[str, ...],
) -> None:
    with pytest.raises(ValueError, match="nonempty and unique"):
        prepare_rich_rows(
            history, native, scope_config=scope_config, native_features=names
        )


def test_colliding_feature_names_rejected(
    history: pl.DataFrame, native: pl.DataFrame, scope_config: CellScopeConfig
) -> None:
    with pytest.raises(ValueError, match="collide"):
        prepare_rich_rows(
            history, native, scope_config=scope_config, native_features=("age",)
        )


def test_duplicate_native_keys_rejected(
    history: pl.DataFrame, native: pl.DataFrame, scope_config: CellScopeConfig
) -> None:
    with pytest.raises(ValueError, match="Duplicate"):
        prepare_rich_rows(
            history,
            pl.concat([native, native.head(1)]),
            scope_config=scope_config,
            native_features=("native_signal",),
        )


def test_missing_labelled_history_rejected(
    history: pl.DataFrame, native: pl.DataFrame, scope_config: CellScopeConfig
) -> None:
    with pytest.raises(ValueError, match="required labelled"):
        prepare_rich_rows(
            history,
            native.tail(5),
            scope_config=scope_config,
            native_features=("native_signal",),
        )


def test_missing_unlabelled_history_is_retained(
    history: pl.DataFrame, native: pl.DataFrame, scope_config: CellScopeConfig
) -> None:
    amended = history.with_columns(
        pl.when((pl.col("venue") == "81") & (pl.col("horse_number") == 1))
        .then(None)
        .otherwise(pl.col("finish"))
        .alias("finish")
    )
    rows = prepare_rich_rows(
        amended,
        native.tail(5),
        scope_config=scope_config,
        native_features=("native_signal",),
    )
    assert rows.audit["missing_native_unlabelled"] == 1
    assert rows.training.height == 4


def test_label_disagreement_rejected(
    history: pl.DataFrame, native: pl.DataFrame, scope_config: CellScopeConfig
) -> None:
    with pytest.raises(ValueError, match="labels disagree"):
        prepare_rich_rows(
            history,
            native.with_columns(pl.lit(9).alias("native_finish")),
            scope_config=scope_config,
            native_features=("native_signal",),
        )


@pytest.mark.parametrize("early_only", [True, False])
def test_early_card_policy_excludes_unpublished_values(early_only: bool) -> None:
    config = RichConfig(
        native_features=(
            "native_signal",
            "odds_score",
            "popularity_score",
            "weight_diff_from_avg",
            "weather_normalized",
            "horse_baba_win_rate",
            "weight_avg_5",
            "sim_fav_win_rate",
        ),
        include_history=True,
        early_only=early_only,
    )
    names = rich_feature_names(config)
    assert ("odds_score" in names) is not early_only
    assert ("horse_baba_win_rate" in names) is not early_only
    assert ("going" in names) is not early_only
    assert "weight_avg_5" in names
    assert "sim_fav_win_rate" in names
    assert "past_speed_mean" in names


def test_numeric_projection_preserves_nan_and_coerces_null() -> None:
    frame = pl.DataFrame({"x": [None, float("nan"), 1.25]})
    np.testing.assert_array_equal(
        rich_matrix(frame, ("x",)), np.array([[0.0], [np.nan], [1.25]])
    )


@pytest.mark.parametrize("include_history", [False, True])
def test_fixed_native_objective_fit(
    history: pl.DataFrame,
    native: pl.DataFrame,
    scope_config: CellScopeConfig,
    include_history: bool,
) -> None:
    rows = prepare_rich_rows(
        history, native, scope_config=scope_config, native_features=("native_signal",)
    )
    model, predictions, features = train_rich_fold(
        rows,
        config=RichConfig(
            native_features=("native_signal",),
            include_history=include_history,
            iterations=2,
            depth=2,
        ),
    )
    assert model.tree_count_ == 2
    assert predictions["predicted_rank"].to_list() == [1, 2]
    assert features[0] == "native_signal"


@pytest.mark.parametrize("empty", [True, False])
def test_invalid_training_split_rejected(
    history: pl.DataFrame,
    native: pl.DataFrame,
    scope_config: CellScopeConfig,
    empty: bool,
) -> None:
    rows = prepare_rich_rows(
        history, native, scope_config=scope_config, native_features=("native_signal",)
    )
    bad = replace(rows, training=rows.training.head(0) if empty else rows.evaluation)
    with pytest.raises(ValueError, match="splits|strictly precede"):
        train_rich_fold(
            bad,
            config=RichConfig(
                native_features=("native_signal",), include_history=False, iterations=2
            ),
        )


def test_cli_retains_scope_and_caveat(history: pl.DataFrame, tmp_path: Path) -> None:
    history.drop(
        "past_relative_speed_mean",
        "past_relative_speed_365d",
        "past_relative_speed_28d",
    ).write_parquet(tmp_path / "history.parquet")
    native_raw = history.select(
        pl.lit("nar").alias("source"),
        "race_date",
        pl.col("venue").alias("keibajo_code"),
        pl.lit("01").alias("race_bango"),
        pl.col("horse_id").alias("ketto_toroku_bango"),
        pl.col("finish").alias("finish_position"),
        (3 - pl.col("horse_number")).alias("native_signal"),
    )
    native_raw.filter(pl.col("keibajo_code") == "83").write_parquet(
        tmp_path / "native.parquet"
    )
    native_raw.filter(pl.col("keibajo_code") == "81").write_parquet(
        tmp_path / "retired.parquet"
    )
    (tmp_path / "metadata.json").write_text(
        json.dumps({"feature_names": ["native_signal"]}), encoding="utf-8"
    )
    main(
        [
            "--history",
            str(tmp_path / "history.parquet"),
            "--native",
            str(tmp_path / "native.parquet"),
            "--retired",
            str(tmp_path / "retired.parquet"),
            "--metadata",
            str(tmp_path / "metadata.json"),
            "--output",
            str(tmp_path / "out"),
            "--year",
            "2024",
            "--iterations",
            "2",
            "--include-history",
        ]
    )
    report = json.loads((tmp_path / "out" / "report.json").read_text(encoding="utf-8"))
    assert report["seed_years"] == 20
    assert report["promotion_eligible"] is False
    assert (tmp_path / "out" / "seed-races.parquet").is_file()
