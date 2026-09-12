from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
from learning.prepare_cell_timesfm import (
    align_cell_baseline,
    attach_body_measurements,
    attach_cell_dimensions,
    main,
    temporal_observations,
)


@pytest.fixture
def history() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "race_id": ["r0", "r0", "r1", "r1", "r2", "r2"],
            "horse_id": ["a", "b"] * 3,
            "race_date": ["20190901"] * 2 + ["20200901"] * 2 + ["20260901"] * 2,
            "category": ["ban-ei"] * 6,
            "venue": ["83"] * 6,
            "horse_number": [1, 2] * 3,
            "finish": [1, 2] * 3,
            "distance_m": [200.0] * 6,
            "field_size": [2.0] * 6,
            "clock_seconds": [100.0, 110.0] * 3,
            "track_code": [1] * 6,
        }
    )


def _metadata(history: pl.DataFrame) -> pl.DataFrame:
    return (
        history.select("race_id", "venue")
        .unique()
        .with_columns(
            pl.lit("C").alias("class_label"),
            pl.lit("sprint").alias("distance_band"),
            pl.lit("autumn").alias("season"),
            pl.lit("dirt").alias("surface"),
        )
    )


def test_dimensions_and_physical_observations(history: pl.DataFrame) -> None:
    enriched = attach_cell_dimensions(history, _metadata(history))
    assert enriched.height == 6
    values = temporal_observations(enriched)
    first = values.filter(pl.col("race_id") == "r0").sort("horse_id")
    assert first["performance"].to_list() == [1.0, 0.0]
    assert first["relative_speed"].to_list() == pytest.approx(
        [0.707106781, -0.707106781]
    )


def test_exchange_copies_use_native_observation_without_losing_competitors(
    history: pl.DataFrame,
) -> None:
    native = history.with_columns(
        pl.concat_str(pl.lit("nar-"), pl.col("race_id")).alias("race_id")
    )
    exchange = history.filter(pl.col("horse_id") == "a").with_columns(
        pl.concat_str(pl.lit("jra-"), pl.col("race_id")).alias("race_id"),
        pl.lit("jra").alias("category"),
        pl.lit(2, dtype=pl.Int64).alias("finish"),
    )
    result = temporal_observations(pl.concat([exchange, native]))
    assert result.height == 6
    assert result.filter(pl.col("horse_id") == "a")["performance"].to_list() == [
        1.0,
        1.0,
        1.0,
    ]


def test_day_speed_preserves_between_race_pace_and_separates_tracks(
    history: pl.DataFrame,
) -> None:
    shared = history.with_columns(
        pl.lit("20190901").alias("race_date"),
        pl.Series("clock_seconds", [100.0, 110.0, 200.0, 220.0, 0.0, 0.0]),
    )
    result = temporal_observations(shared)
    assert result.filter((pl.col("race_id") == "r0") & (pl.col("horse_id") == "a"))[
        "day_speed"
    ].item() == pytest.approx(0.39422868)
    assert (
        result.filter((pl.col("race_id") == "r1") & (pl.col("horse_id") == "a"))[
            "day_speed"
        ].item()
        < 0
    )
    separated = temporal_observations(
        shared.with_columns(
            pl.when(pl.col("race_id") == "r1").then(2).otherwise(1).alias("track_code")
        )
    )
    assert separated.filter((pl.col("race_id") == "r0") & (pl.col("horse_id") == "a"))[
        "day_speed"
    ].item() == pytest.approx(0.04765509)


def test_actual_body_measurements_decode_and_preserve_missing(
    history: pl.DataFrame,
) -> None:
    observations = temporal_observations(history)
    raw = pl.DataFrame(
        {
            "race_id": ["nar-r0", "r1", "r2"],
            "horse_id": ["a"] * 3,
            "raw_body": ["3E5", "FFF", "000"],
        }
    )
    result = attach_body_measurements(observations, raw)
    assert result.height == 6
    assert result["body_weight"].drop_nulls().to_list() == pytest.approx(
        [-0.0030045090202987243]
    )
    assert result["body_weight"].null_count() == 5
    with pytest.raises(ValueError, match="Duplicate physical"):
        attach_body_measurements(observations, pl.concat([raw, raw]))


def test_invalid_performance_and_zero_variance_are_missing(
    history: pl.DataFrame,
) -> None:
    invalid = history.with_columns(
        pl.lit(1.0).alias("field_size"), pl.lit(0.0).alias("clock_seconds")
    )
    result = temporal_observations(invalid)
    assert result["performance"].null_count() == 6
    assert result["relative_speed"].null_count() == 6


@pytest.mark.parametrize("duplicate", [True, False])
def test_metadata_integrity(history: pl.DataFrame, duplicate: bool) -> None:
    metadata = _metadata(history)
    bad = pl.concat([metadata, metadata.head(1)]) if duplicate else metadata.head(1)
    with pytest.raises(ValueError, match="Duplicate|incomplete"):
        attach_cell_dimensions(history, bad)


def test_jra_exchange_records_remain_history_without_nar_metadata(
    history: pl.DataFrame,
) -> None:
    exchange = history.with_columns(pl.lit("jra").alias("category"))
    result = attach_cell_dimensions(exchange, _metadata(history).head(0))
    assert result.height == 6
    assert result["class_label"].unique().to_list() == ["__unmapped_history__"]
    assert result["cell_metadata_available"].to_list() == [False] * 6


def test_baseline_normalization(history: pl.DataFrame) -> None:
    baseline = history.with_columns(pl.Series("score", [2.0, 1.0] * 3))
    result = align_cell_baseline(history, baseline)
    assert result["baseline_score"].to_list() == [1.0, 0.0, 1.0, 0.0, 1.0, 0.0]


@pytest.mark.parametrize("mismatch", [True, False])
def test_baseline_integrity(history: pl.DataFrame, mismatch: bool) -> None:
    baseline = history.with_columns(pl.lit(1.0).alias("score"))
    bad = (
        baseline.with_columns(pl.lit(9).alias("finish"))
        if mismatch
        else baseline.head(1)
    )
    with pytest.raises(ValueError, match="labels disagree|Missing"):
        align_cell_baseline(history, bad)


@pytest.mark.parametrize("raw_body", [False, True])
@pytest.mark.parametrize("venue,year", [("83", 2020), ("54", 2020), ("54", 2026)])
def test_cli_separate_seed_scope_and_shared_control(
    history: pl.DataFrame, tmp_path: Path, venue: str, year: int, raw_body: bool
) -> None:
    category = "ban-ei" if venue == "83" else "nar"
    history = history.with_columns(
        pl.lit(venue).alias("venue"), pl.lit(category).alias("category")
    )
    history.write_parquet(tmp_path / "history.parquet")
    history.select("race_id", "horse_id").with_columns(
        pl.lit("3E5").alias("raw_body")
    ).write_csv(tmp_path / "body.csv")
    _metadata(history).write_parquet(tmp_path / "race-cells-local-pg.parquet")
    (tmp_path / "target-cells.json").write_text(
        json.dumps(
            [
                {
                    "category": category,
                    "venue": venue,
                    "class_label": "C",
                    "distance_band": "sprint",
                    "season": "autumn",
                    "surface": "dirt",
                    "scheduled_races": 1,
                }
            ]
        ),
        encoding="utf-8",
    )
    if venue == "83":
        model = tmp_path / "rich-early-body-ablation-v1/83/2020/native"
    elif year == 2026:
        model = tmp_path / "full-2026-evaluation-v1/54/speed"
    else:
        model = tmp_path / "ablation-v1/54/2020/speed"
    model.mkdir(parents=True)
    history.with_columns(pl.lit(1.0).alias("score")).write_parquet(
        model / "predictions.parquet"
    )
    main(
        [
            *(["--raw-body", str(tmp_path / "body.csv")] if raw_body else []),
            "--history",
            str(tmp_path / "history.parquet"),
            "--root",
            str(tmp_path),
            "--output",
            str(tmp_path / "prepared"),
            "--years",
            str(year),
        ]
    )
    result = pl.read_parquet(tmp_path / "prepared/targets.parquet")
    assert result.height == 2
    report_path = next((tmp_path / "prepared/cells").glob("*/*/scope.json"))
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["seed_years"] == 20
    assert report["promotion_eligible"] is False
