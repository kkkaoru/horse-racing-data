"""Chronological exact-cell Rustuna campaign over immutable forecast caches."""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict
from pathlib import Path

import numpy as np
import polars as pl

from .nar_banei_campaign import DEFAULT_PROFILES, PROFILES, file_digest
from .nar_banei_exact import Readout, exact_hits, optimize_readout, predicted_ranks, readout_scores

IDENTITY = ("race_id", "race_date", "horse_id", "horse_number", "finish")


def load_cache(
    targets: pl.DataFrame, cache: Path, profiles: list[str]
) -> tuple[dict[str, dict[int, pl.DataFrame]], dict[str, str]]:
    folds: dict[str, dict[int, pl.DataFrame]] = {}
    hashes: dict[str, str] = {}
    for profile in profiles:
        folds[profile] = {}
        for key, expected in targets.partition_by("year", as_dict=True).items():
            year = int(key[0])
            cell = str(expected["cell_id"][0])
            path = cache / profile / cell / str(year) / "forecasts.parquet"
            hashes[str(path)] = file_digest(path)
            hashes[str(path.with_name("report.json"))] = file_digest(path.with_name("report.json"))
            frame = pl.read_parquet(path)
            signature = [*IDENTITY, "baseline_score", "year", "cell_id"]
            if (
                not frame.select(signature)
                .sort("race_id", "horse_id")
                .equals(expected.select(signature).sort("race_id", "horse_id"))
            ):
                raise ValueError("Forecast cache does not match the complete expected cohort")
            folds[profile][year] = frame
    return folds, hashes


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--targets", type=Path, required=True)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--profiles", nargs="+", choices=tuple(PROFILES), default=list(DEFAULT_PROFILES)
    )
    parser.add_argument(
        "--origins",
        nargs="+",
        choices=("timesfm", "last", "mean5"),
        default=["timesfm", "last", "mean5"],
    )
    parser.add_argument("--years", nargs="+", type=int, default=list(range(2020, 2027)))
    parser.add_argument("--cells", nargs="+")
    parser.add_argument("--trials", type=int, default=300)
    parser.add_argument("--seed", type=int, default=20260912)
    parser.add_argument(
        "--normalizations", nargs="+", choices=("rank", "centered", "innovation"), default=["rank"]
    )
    parser.add_argument("--half-life-days", nargs="+", type=int, default=[0])
    args = parser.parse_args(argv)
    if any(value < 0 for value in args.half_life_days):
        parser.error("Half-lives must be nonnegative")
    if args.trials < 1:
        parser.error("Trials must be positive")
    targets = pl.read_parquet(args.targets)
    if args.cells:
        targets = targets.filter(pl.col("cell_id").is_in(args.cells))
    if targets.is_empty():
        raise ValueError("No matching cells")
    code_hash = file_digest(Path(__file__).with_name("nar_banei_exact.py"))
    for cell_frame in targets.partition_by("cell_id"):
        cell = str(cell_frame["cell_id"][0])
        folds, hashes = load_cache(cell_frame, args.cache, args.profiles)
        for origin in args.origins:
            for year in args.years:
                output = args.output / origin / cell / str(year)
                if (output / "report.json").exists():
                    raise ValueError(
                        "Output already exists; do not overwrite a completed selection"
                    )
                development = {
                    profile: [
                        frame for fold_year, frame in sorted(by_year.items()) if fold_year < year
                    ]
                    for profile, by_year in folds.items()
                }
                started = time.perf_counter()
                if development[args.profiles[0]]:
                    config, trials = optimize_readout(
                        development,
                        origin=origin,
                        evaluation_year=year,
                        n_trials=args.trials,
                        seed=args.seed,
                        normalizations=tuple(args.normalizations),
                        half_lives=tuple(args.half_life_days),
                    )
                else:
                    config, trials = Readout(args.profiles[0], origin), []
                elapsed = time.perf_counter() - started
                frame = folds[config.profile].get(year)
                hits = baseline_hits = None
                races = dates = rows = 0
                output.mkdir(parents=True, exist_ok=True)
                if frame is not None:
                    scores = readout_scores(frame, config)
                    baseline = np.asarray(frame["baseline_score"].to_numpy(), dtype=np.float64)
                    hits, baseline_hits = (
                        exact_hits(frame, scores).tolist(),
                        exact_hits(frame, baseline).tolist(),
                    )
                    rows, races, dates = (
                        frame.height,
                        frame["race_id"].n_unique(),
                        frame["race_date"].n_unique(),
                    )
                    frame.select(*IDENTITY).with_columns(
                        pl.Series("score", scores),
                        pl.Series("predicted_rank", predicted_ranks(frame, scores)),
                    ).write_parquet(output / "predictions.parquet")
                    frame.select(*IDENTITY).with_columns(
                        pl.Series("score", baseline),
                        pl.Series("predicted_rank", predicted_ranks(frame, baseline)),
                    ).write_parquet(output / "baseline.parquet")
                report = {
                    "cell_id": cell,
                    "evaluation_year": year,
                    "origin": origin,
                    "config": asdict(config),
                    "development_years": sorted(
                        fold_year for fold_year in folds[config.profile] if fold_year < year
                    ),
                    "trials": len(trials),
                    "elapsed_seconds": elapsed,
                    "trials_per_second": len(trials) / elapsed if elapsed else 0.0,
                    "rows": rows,
                    "races": races,
                    "dates": dates,
                    "exact_hits": hits,
                    "baseline_exact_hits": baseline_hits,
                    "cold_start": not trials,
                    "source_hashes": hashes,
                    "readout_code_sha256": code_hash,
                    "seed": args.seed,
                    "normalizations": args.normalizations,
                    "half_life_days": args.half_life_days,
                    "baseline_kind": "local-PG chronological shared-venue research control",
                    "research_only": True,
                    "noncommercial_personal_validation": True,
                    "serving_parity_verified": False,
                    "promotion_eligible": False,
                }
                (output / "trials.json").write_text(json.dumps(trials, indent=2), encoding="utf-8")
                (output / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
                print(
                    json.dumps(
                        {key: value for key, value in report.items() if key != "source_hashes"}
                    ),
                    flush=True,
                )


if __name__ == "__main__":
    main()
