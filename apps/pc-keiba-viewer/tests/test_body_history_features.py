from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
from learning.body_history_features import (
    body_weight_expression,
    build_body_history,
    main,
)


def test_hexadecimal_mass_not_exponential_and_sentinels() -> None:
    values = pl.DataFrame(
        {"raw": ["3E5", "3E0", " 3e8 ", "FFF", "000", None, "bad!", "1234", "", "438"]}
    )
    assert values.select(body_weight_expression("raw")).to_series().to_list() == [
        997,
        992,
        1000,
        None,
        None,
        None,
        None,
        None,
        None,
        1080,
    ]


@pytest.fixture
def history() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "race_id": [
                "nar-20260101-83-01",
                "nar-20260102-83-01",
                "nar-20260103-83-01",
            ],
            "race_date": ["20260101", "20260102", "20260103"],
            "horse_id": ["a", "a", "a"],
            "venue": ["83", "83", "83"],
            "finish": [1, 1, 1],
        }
    )


@pytest.fixture
def raw(history: pl.DataFrame) -> pl.DataFrame:
    return history.select("race_id", "horse_id").with_columns(
        pl.Series("raw_body", ["3E0", "3E5", "3E6"])
    )


def test_past_only_and_same_day_exclusion(
    history: pl.DataFrame, raw: pl.DataFrame
) -> None:
    forecast = history.tail(1).with_columns(
        pl.lit("forecast").alias("race_id"),
        pl.lit("20260102").alias("race_date"),
        pl.lit(None).cast(pl.Int64).alias("finish"),
    )
    result = build_body_history(pl.concat([history, forecast]), raw)
    assert (
        result.filter(pl.col("race_id") == "forecast")["corrected_weight_avg_5"].item()
        == 992.0
    )
    assert (
        result.filter(pl.col("race_id") == "nar-20260101-83-01")[
            "corrected_weight_avg_5"
        ].item()
        is None
    )
    assert (
        result.filter(pl.col("race_id") == "nar-20260103-83-01")[
            "corrected_weight_avg_5"
        ].item()
        == 994.5
    )


@pytest.mark.parametrize("duplicate_raw", [True, False])
def test_duplicate_keys_rejected(
    history: pl.DataFrame, raw: pl.DataFrame, duplicate_raw: bool
) -> None:
    with pytest.raises(ValueError, match="Duplicate"):
        build_body_history(
            history if duplicate_raw else pl.concat([history, history.head(1)]),
            pl.concat([raw, raw.head(1)]) if duplicate_raw else raw,
        )


def test_ambiguous_same_day_classified_starts_rejected(
    history: pl.DataFrame, raw: pl.DataFrame
) -> None:
    with pytest.raises(ValueError, match="Multiple classified"):
        build_body_history(
            history.with_columns(pl.lit("20260101").alias("race_date")), raw
        )


def test_missing_weight_counts_toward_five_start_window() -> None:
    history = pl.DataFrame(
        {
            "race_id": ["1", "2", "3", "4", "5", "6", "7"],
            "horse_id": ["a"] * 7,
            "race_date": [
                "20260101",
                "20260102",
                "20260103",
                "20260104",
                "20260105",
                "20260106",
                "20260107",
            ],
            "finish": [1] * 7,
        }
    )
    raw = history.select("race_id", "horse_id").with_columns(
        pl.Series("raw_body", ["3E0", "FFF", "3E2", "3E3", "3E4", "3E5", "3E6"])
    )
    result = build_body_history(history, raw)
    assert (
        result.filter(pl.col("race_id") == "7")["corrected_weight_avg_5"].item()
        == 995.5
    )


def _native(history: pl.DataFrame) -> pl.DataFrame:
    return history.select(
        pl.lit("nar").alias("source"),
        "race_date",
        pl.col("venue").alias("keibajo_code"),
        pl.lit("01").alias("race_bango"),
        pl.col("horse_id").alias("ketto_toroku_bango"),
        pl.col("finish").alias("finish_position"),
        pl.lit(300000.0).alias("weight_avg_5"),
    )


def test_cli_preserves_legacy_feature_and_separate_contract(
    history: pl.DataFrame, raw: pl.DataFrame, tmp_path: Path
) -> None:
    history.write_parquet(tmp_path / "history.parquet")
    raw.write_csv(tmp_path / "body.csv")
    _native(history.head(2)).write_parquet(tmp_path / "native.parquet")
    _native(history.tail(1)).write_parquet(tmp_path / "retired.parquet")
    upcoming = _native(history.tail(1)).with_columns(
        pl.lit("20260104").alias("race_date"),
        pl.lit(None).cast(pl.Int64).alias("finish_position"),
    )
    upcoming.write_parquet(tmp_path / "upcoming.parquet")
    (tmp_path / "metadata.json").write_text(
        json.dumps({"feature_names": ["weight_avg_5"]}), encoding="utf-8"
    )
    main(
        [
            "--history",
            str(tmp_path / "history.parquet"),
            "--body-csv",
            str(tmp_path / "body.csv"),
            "--native",
            str(tmp_path / "native.parquet"),
            "--retired",
            str(tmp_path / "retired.parquet"),
            "--upcoming",
            str(tmp_path / "upcoming.parquet"),
            "--metadata",
            str(tmp_path / "metadata.json"),
            "--output",
            str(tmp_path / "out"),
        ]
    )
    result = pl.read_parquet(tmp_path / "out" / "upcoming.parquet")
    assert result["weight_avg_5"].item() == 300000.0
    assert result["corrected_weight_avg_5"].item() == pytest.approx(
        (992 + 997 + 998) / 3
    )
    contract = json.loads(
        (tmp_path / "out" / "metadata.json").read_text(encoding="utf-8")
    )
    assert contract["feature_names"] == ["corrected_weight_avg_5"]
