"""Matched exact-rank comparisons with date-cluster bootstrap uncertainty."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Final

import numpy as np
import polars as pl

RANKS: Final[tuple[int, ...]] = (1, 2, 3, 4, 5)
IDENTITY: Final[tuple[str, ...]] = (
    "race_id",
    "race_date",
    "horse_id",
    "horse_number",
    "finish",
)
MINIMUM_DATES: Final[int] = 20


@dataclass(frozen=True)
class PairedRankReport:
    races: int
    dates: int
    baseline_accuracy: list[float]
    candidate_accuracy: list[float]
    deltas: list[float]
    observed_rank_support: list[int]
    lower: list[float] | None
    upper: list[float] | None
    uncertainty: str
    bootstrap_samples: int
    seed: int
    promotion_eligible: bool = False


def validate_predictions(frame: pl.DataFrame) -> pl.DataFrame:
    """Reject incomplete identities and non-permutation predicted ranks."""
    selected = frame.select(*IDENTITY, "predicted_rank").sort("race_id", "horse_number")
    if selected.is_empty() or selected.null_count().sum_horizontal().item() > 0:
        raise ValueError(
            "Nonempty predictions with complete identities and labels are required"
        )
    if selected.select("race_id", "horse_number").is_duplicated().any():
        raise ValueError("Duplicate race/horse predictions")
    numeric = selected.select("finish", "predicted_rank").cast(pl.Float64)
    invalid = numeric.select(
        pl.all().is_finite() & (pl.all() >= 1) & (pl.all() == pl.all().floor())
    )
    if not invalid.select(pl.all_horizontal(pl.all()).all()).item():
        raise ValueError("Finish and predicted rank must be positive finite integers")
    groups = selected.group_by("race_id").agg(
        pl.len().alias("n"),
        pl.col("predicted_rank").n_unique().alias("unique"),
        pl.col("predicted_rank").max().alias("maximum"),
        pl.col("race_date").n_unique().alias("dates"),
    )
    if groups.filter(
        (pl.col("n") != pl.col("unique"))
        | (pl.col("n") != pl.col("maximum"))
        | (pl.col("dates") != 1)
    ).height:
        raise ValueError("Each race needs one date and a predicted-rank permutation")
    return selected


def race_hits(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.group_by("race_id", "race_date").agg(
        [
            ((pl.col("finish") == rank) & (pl.col("predicted_rank") == rank))
            .any()
            .cast(pl.Int32)
            .alias(f"rank{rank}")
            for rank in RANKS
        ]
    )


def compare_rank_predictions(
    baseline: pl.DataFrame,
    candidate: pl.DataFrame,
    *,
    samples: int = 10000,
    seed: int = 20260912,
) -> PairedRankReport:
    """Bootstrap dates, weighting race sums, not unweighted daily accuracies.

    Percentile tails are 0.005/0.995 (Bonferroni across five ranks). These
    approximate intervals do not correct across candidate searches or cells.
    Fewer than twenty date clusters yields point estimates only. This function
    cannot certify training provenance or authorize production promotion.
    """
    if samples < 1000:
        raise ValueError("At least 1000 bootstrap samples are required")
    base = validate_predictions(baseline)
    trial = validate_predictions(candidate)
    if not base.select(IDENTITY).equals(trial.select(IDENTITY)):
        raise ValueError(
            "Baseline and candidate must have identical entrants, dates and labels"
        )
    columns = [f"rank{rank}" for rank in RANKS]
    joined = race_hits(base).join(
        race_hits(trial), on=["race_id", "race_date"], suffix="_candidate"
    )
    base_accuracy = joined.select(columns).to_numpy().mean(axis=0)
    trial_accuracy = (
        joined.select([f"{column}_candidate" for column in columns])
        .to_numpy()
        .mean(axis=0)
    )
    daily = (
        joined.group_by("race_date")
        .agg(
            pl.len().alias("races"),
            *[
                (pl.col(f"{column}_candidate") - pl.col(column)).sum().alias(column)
                for column in columns
            ],
        )
        .sort("race_date")
    )
    lower = upper = None
    uncertainty = "insufficient-date-clusters"
    if daily.height >= MINIMUM_DATES:
        rng = np.random.default_rng(seed)
        weights = rng.multinomial(
            daily.height, np.full(daily.height, 1 / daily.height), size=samples
        )
        draws = (weights @ daily.select(columns).to_numpy()) / (
            weights @ daily["races"].to_numpy()
        )[:, None]
        bounds = np.quantile(draws, [0.005, 0.995], axis=0)
        lower, upper = bounds[0].tolist(), bounds[1].tolist()
        uncertainty = "date-cluster-percentile-bonferroni-five-ranks-approximate"
    support = [
        base.filter(pl.col("finish") == rank)["race_id"].n_unique() for rank in RANKS
    ]
    return PairedRankReport(
        races=joined.height,
        dates=daily.height,
        baseline_accuracy=base_accuracy.tolist(),
        candidate_accuracy=trial_accuracy.tolist(),
        deltas=(trial_accuracy - base_accuracy).tolist(),
        observed_rank_support=support,
        lower=lower,
        upper=upper,
        uncertainty=uncertainty,
        bootstrap_samples=samples,
        seed=seed,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    report = compare_rank_predictions(
        pl.read_parquet(args.baseline), pl.read_parquet(args.candidate)
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(asdict(report), indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
