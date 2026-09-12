"""Causal Ban-ei body history, decoding three-character hexadecimal kilograms."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import polars as pl


def body_weight_expression(column: str) -> pl.Expr:
    """Match the existing raw-feature hex contract; FFF/zero are unavailable."""
    raw = pl.col(column).str.strip_chars().str.to_uppercase()
    value = raw.str.to_integer(base=16, strict=False)
    return (
        pl.when(raw.str.contains(r"^[0-9A-F]{1,3}$") & (raw != "FFF") & (value > 0))
        .then(value)
        .otherwise(None)
        .alias("body_kg")
    )


def build_body_history(history: pl.DataFrame, raw_body: pl.DataFrame) -> pl.DataFrame:
    """Last five classified starts, including missing weights; never same-day."""
    keys = ["race_id", "horse_id"]
    if (
        history.select(keys).is_duplicated().any()
        or raw_body.select(keys).is_duplicated().any()
    ):
        raise ValueError("Duplicate race/horse keys in body history")
    source = history.with_columns(
        pl.col("race_date").str.strptime(pl.Date, "%Y%m%d").alias("body_date")
    )
    observations = (
        source.filter(pl.col("finish") > 0)
        .join(
            raw_body.with_columns(body_weight_expression("raw_body")),
            on=keys,
            how="left",
            validate="m:1",
        )
        .sort("horse_id", "body_date")
    )
    if observations.select("horse_id", "body_date").is_duplicated().any():
        raise ValueError("Multiple classified starts for one horse/day are ambiguous")
    statistics = observations.with_columns(
        pl.col("body_kg")
        .rolling_mean(window_size=5, min_samples=1)
        .over("horse_id")
        .alias("corrected_weight_avg_5")
    ).select(
        "horse_id",
        pl.col("body_date").alias("history_body_date"),
        "corrected_weight_avg_5",
    )
    return (
        source.sort("horse_id", "body_date")
        .join_asof(
            statistics,
            left_on="body_date",
            right_on="history_body_date",
            by="horse_id",
            strategy="backward",
            allow_exact_matches=False,
            check_sortedness=False,
        )
        .select(*keys, "corrected_weight_avg_5")
    )


def append_native_body_history(
    native: pl.DataFrame, patch: pl.DataFrame
) -> pl.DataFrame:
    """New candidate data only: preserve legacy artifacts and all native rows."""
    normalized = native.with_columns(
        pl.concat_str(
            "source", "race_date", "keibajo_code", "race_bango", separator="-"
        ).alias("race_id"),
        pl.col("ketto_toroku_bango").alias("horse_id"),
    )
    return normalized.join(
        patch,
        on=["race_id", "horse_id"],
        how="left",
        validate="m:1",
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", type=Path, required=True)
    parser.add_argument("--body-csv", type=Path, required=True)
    parser.add_argument("--native", required=True)
    parser.add_argument("--retired", required=True)
    parser.add_argument("--upcoming", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--metadata", type=Path, required=True)
    args = parser.parse_args(argv)
    history = pl.read_parquet(
        args.history, columns=["race_id", "race_date", "horse_id", "venue", "finish"]
    ).filter(pl.col("venue").is_in(["81", "82", "83", "84"]))
    upcoming = pl.read_parquet(args.upcoming)
    targets = upcoming.select(
        pl.concat_str(
            "source", "race_date", "keibajo_code", "race_bango", separator="-"
        ).alias("race_id"),
        "race_date",
        pl.col("ketto_toroku_bango").alias("horse_id"),
        pl.col("keibajo_code").alias("venue"),
        pl.col("finish_position").alias("finish"),
    )
    raw_body = pl.read_csv(
        args.body_csv,
        schema_overrides={
            "race_id": pl.String,
            "horse_id": pl.String,
            "raw_body": pl.String,
        },
    )
    patch = build_body_history(
        pl.concat([history, targets], how="vertical_relaxed"), raw_body
    )
    args.output.mkdir(parents=True, exist_ok=True)
    patch.write_parquet(args.output / "causal-body-history.parquet")
    append_native_body_history(pl.read_parquet(args.native), patch).write_parquet(
        args.output / "native.parquet"
    )
    append_native_body_history(pl.read_parquet(args.retired), patch).write_parquet(
        args.output / "retired.parquet"
    )
    append_native_body_history(upcoming, patch).write_parquet(
        args.output / "upcoming.parquet"
    )
    original = json.loads(args.metadata.read_text(encoding="utf-8"))
    contract = {
        "feature_names": [
            "corrected_weight_avg_5" if name == "weight_avg_5" else name
            for name in original["feature_names"]
        ],
        "source_metadata": str(args.metadata),
        "role": "research input contract, not a trained model bundle",
    }
    (args.output / "metadata.json").write_text(
        json.dumps(contract, indent=2), encoding="utf-8"
    )
    report = {
        "source": "local-pg nvd_se",
        "raw_rows": raw_body.height,
        "feature_rows": patch.height,
        "minimum": patch["corrected_weight_avg_5"].min(),
        "maximum": patch["corrected_weight_avg_5"].max(),
        "history_contract": "previous-day last five classified starts; null weights count as starts; FFF and zero unavailable",
        "encoding": "hexadecimal kg, not decimal/scientific notation",
        "promotion_eligible": False,
    }
    (args.output / "report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
