"""Prepare local-PG exact-cell scopes and observations for temporal experiments."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date
from pathlib import Path

import polars as pl

from learning.body_history_features import body_weight_expression
from learning.cell_training_scope import CellScopeConfig, build_cell_training_scope

CELL_DIMENSIONS = ("venue", "class_label", "distance_band", "season", "surface")


def attach_cell_dimensions(
    history: pl.DataFrame, metadata: pl.DataFrame
) -> pl.DataFrame:
    if metadata["race_id"].is_duplicated().any():
        raise ValueError("Duplicate race metadata")
    selected = metadata.select(
        "race_id", pl.col("venue").alias("metadata_venue"), *CELL_DIMENSIONS[1:]
    )
    result = history.join(selected, on="race_id", how="left", validate="m:1")
    required = result.filter(
        pl.col("venue").is_in(["54", "55", "83"])
        & pl.col("category").is_in(["nar", "ban-ei"])
    )
    if (
        required["metadata_venue"].null_count()
        or any(required[name].null_count() for name in CELL_DIMENSIONS[1:])
        or required.filter(pl.col("venue") != pl.col("metadata_venue")).height
    ):
        raise ValueError("Target-venue race metadata is incomplete or inconsistent")
    return result.with_columns(
        pl.col("metadata_venue").is_not_null().alias("cell_metadata_available"),
        pl.col(*CELL_DIMENSIONS[1:]).fill_null("__unmapped_history__"),
    ).drop("metadata_venue")


def temporal_observations(history: pl.DataFrame) -> pl.DataFrame:
    """Realized observations, NOT predictors; consumers must enforce past-only queries."""
    # JVD contains exchange-race copies of NVD races. Keep the native NVD
    # observation, retaining JVD-only competitors and earlier regional history.
    canonical = (
        history.with_columns(
            pl.col("race_id").str.replace(r"^(jra|nar)-", "").alias("physical_race_id"),
            (pl.col("category") != "jra").cast(pl.UInt8).alias("source_priority"),
        )
        .sort("source_priority", descending=True)
        .unique(subset=["physical_race_id", "horse_id"], keep="first")
        .drop("race_id", "source_priority")
        .rename({"physical_race_id": "race_id"})
    )
    frame = canonical.with_columns(
        pl.when(pl.col("clock_seconds") > 0)
        .then(pl.col("distance_m") / pl.col("clock_seconds"))
        .otherwise(None)
        .alias("physical_speed")
    )
    frame = frame.with_columns(
        pl.when(pl.col("physical_speed") > 0)
        .then(pl.col("physical_speed").log())
        .otherwise(None)
        .alias("log_speed")
    )
    return frame.select(
        "race_id",
        "horse_id",
        "race_date",
        pl.when(
            (pl.col("field_size") > 1)
            & pl.col("finish").is_between(1, pl.col("field_size"))
        )
        .then((pl.col("field_size") - pl.col("finish")) / (pl.col("field_size") - 1))
        .otherwise(None)
        .alias("performance"),
        pl.when(pl.col("physical_speed").std().over("race_id") > 0)
        .then(
            (pl.col("physical_speed") - pl.col("physical_speed").mean().over("race_id"))
            / pl.col("physical_speed").std().over("race_id")
        )
        .otherwise(None)
        .alias("relative_speed"),
        (
            pl.col("log_speed")
            - pl.col("log_speed")
            .mean()
            .over("venue", "race_date", "distance_m", "track_code")
        ).alias("day_speed"),
    ).sort("horse_id", "race_date", "race_id")


def attach_body_measurements(
    observations: pl.DataFrame, raw_body: pl.DataFrame
) -> pl.DataFrame:
    """Attach realized log kilograms, not lagged averages; query cutoff stays mandatory."""
    keys = ["race_id", "horse_id"]
    decoded = raw_body.with_columns(
        pl.col("race_id").str.replace(r"^(jra|nar)-", ""),
        body_weight_expression("raw_body"),
    ).select(*keys, (pl.col("body_kg") / 1000.0).log().alias("body_weight"))
    if decoded.select(keys).is_duplicated().any():
        raise ValueError("Duplicate physical race/horse body measurements")
    return observations.join(decoded, on=keys, how="left", validate="1:1")


def align_cell_baseline(expected: pl.DataFrame, baseline: pl.DataFrame) -> pl.DataFrame:
    keys = ["race_id", "horse_id"]
    observed = expected.filter(pl.col("finish") > 0).select(
        *keys, "race_date", "horse_number", "finish"
    )
    joined = observed.join(
        baseline.select(*keys, "score", pl.col("finish").alias("baseline_finish")),
        on=keys,
        how="left",
        validate="1:1",
    )
    if joined["score"].null_count() or not joined["score"].is_finite().all():
        raise ValueError("Missing or non-finite baseline observations")
    if joined.filter(pl.col("finish") != pl.col("baseline_finish")).height:
        raise ValueError("Baseline labels disagree")
    joined = joined.sort("race_id", "horse_number")
    return joined.with_columns(
        (
            1.0
            - (
                pl.col("score").rank(method="ordinal", descending=True).over("race_id")
                - 1
            )
            / (pl.len().over("race_id") - 1).clip(lower_bound=1)
        ).alias("baseline_score")
    ).drop("score", "baseline_finish")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--raw-body", type=Path)
    parser.add_argument("--years", type=int, nargs="+", default=list(range(2020, 2027)))
    args = parser.parse_args(argv)
    columns = [
        "race_id",
        "horse_id",
        "race_date",
        "category",
        "venue",
        "horse_number",
        "finish",
        "distance_m",
        "field_size",
        "clock_seconds",
        "track_code",
    ]
    history = attach_cell_dimensions(
        pl.read_parquet(args.history, columns=columns),
        pl.read_parquet(args.root / "race-cells-local-pg.parquet"),
    )
    args.output.mkdir(parents=True, exist_ok=True)
    observations = temporal_observations(history)
    if args.raw_body is not None:
        observations = attach_body_measurements(
            observations, pl.read_csv(args.raw_body, infer_schema=False)
        )
        provenance = {
            "source": str(args.raw_body),
            "sha256": hashlib.sha256(args.raw_body.read_bytes()).hexdigest(),
            "target": "log(hexadecimal-decoded observed kilograms / 1000); not a lagged average",
            "rows": observations.height,
            "available_body_rows": observations["body_weight"].is_not_null().sum(),
        }
        (args.output / "body-source.json").write_text(
            json.dumps(provenance, indent=2), encoding="utf-8"
        )
    observations.write_parquet(args.output / "observations.parquet")
    cells = json.loads((args.root / "target-cells.json").read_text(encoding="utf-8"))
    targets: list[pl.DataFrame] = []
    for cell in cells:
        cell_id = "-".join(str(cell[key]) for key in ("category", *CELL_DIMENSIONS))
        for year in args.years:
            config = CellScopeConfig(
                cell_id=cell_id,
                category=cell["category"],
                dimensions=tuple((key, str(cell[key])) for key in CELL_DIMENSIONS),
                training_cutoff=date(year, 1, 1),
                evaluation_end=min(date(year, 12, 31), date(2026, 9, 11)),
            )
            scope = build_cell_training_scope(history, config=config)
            if cell["venue"] == "83":
                model = (
                    args.root
                    / "rich-early-body-ablation-v1"
                    / "83"
                    / str(year)
                    / "native"
                )
            elif year == 2026:
                model = args.root / "full-2026-evaluation-v1" / cell["venue"] / "speed"
            else:
                model = args.root / "ablation-v1" / cell["venue"] / str(year) / "speed"
            aligned = align_cell_baseline(
                scope.evaluation_rows, pl.read_parquet(model / "predictions.parquet")
            )
            targets.append(
                aligned.with_columns(
                    pl.lit(cell_id).alias("cell_id"), pl.lit(year).alias("year")
                )
            )
            output = args.output / "cells" / cell_id / str(year)
            output.mkdir(parents=True, exist_ok=True)
            scope.seed_race_ids.write_parquet(output / "seed-races.parquet")
            scope.race_universe.write_parquet(output / "scope-races.parquet")
            report = {
                "cell": cell,
                "year": year,
                "seed_years": 20,
                "training_rows": scope.training_rows.height,
                "training_start": scope.training_rows["race_date"].min(),
                "training_end": scope.training_rows["race_date"].max(),
                "evaluation_rows": aligned.height,
                "evaluation_races": aligned["race_id"].n_unique(),
                "baseline_model": str(model),
                "baseline_scope": "shared venue model; broader than this exact cell scope",
                "source": "local-pg",
                "promotion_eligible": False,
            }
            (output / "scope.json").write_text(
                json.dumps(report, indent=2), encoding="utf-8"
            )
            print(json.dumps(report), flush=True)
    pl.concat(targets).write_parquet(args.output / "targets.parquet")


if __name__ == "__main__":
    main()
