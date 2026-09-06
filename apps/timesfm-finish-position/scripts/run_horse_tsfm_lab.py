#!/usr/bin/env python3
"""Evaluate frozen TimesFM 3.0 or Chronos-2 on pre-year horse histories."""

from __future__ import annotations

import argparse
import json
import platform
from dataclasses import asdict
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from timesfm_finish_position.chronos_forecasting import Chronos2Forecaster
from timesfm_finish_position.forecasting import (
    TemporalForecaster,
    TimesFm3Forecaster,
    resolve_timesfm_device,
)
from timesfm_finish_position.horse_tsfm import (
    HorseYearForecast,
    build_horse_year_queries,
    forecast_horse_year,
)
from timesfm_finish_position.lab_domain import PredictionFrame
from timesfm_finish_position.lab_metrics import evaluate_prediction_frame
from timesfm_finish_position.tabular_evaluation import (
    TreeFoldResult,
    fit_temperature,
    scores_to_probabilities,
    write_prediction_frame,
)

VALUE_COLUMNS = ("performance_rating", "speed_figure", "final_3f_rating", "pace_rating")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", type=Path, default=Path("tmp/nar-horse-history-2020-2026.parquet")
    )
    parser.add_argument("--output", type=Path, default=Path("tmp/horse-tsfm-lab"))
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--backend", choices=("timesfm3", "chronos2"), required=True)
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--accept-non-commercial-license", action="store_true")
    return parser.parse_args()


def load_source(path: Path) -> dict[str, np.ndarray]:
    table = pq.read_table(
        path,
        columns=[
            "race_id",
            "race_date",
            "horse_id",
            "finish_position",
            "decimal_odds",
            *VALUE_COLUMNS,
        ],
    )
    return {name: np.asarray(table.column(name).to_pylist()) for name in table.column_names}


def forecast(
    source: dict[str, np.ndarray], year: int, forecaster: TemporalForecaster
) -> HorseYearForecast:
    history_values = np.column_stack([source[name].astype(np.float64) for name in VALUE_COLUMNS])
    prior = source["race_date"].astype(np.str_) < f"{year}0101"
    fallback = np.mean(history_values[prior], axis=0)
    queries = build_horse_year_queries(
        horse_ids=source["horse_id"].astype(np.str_),
        race_dates=source["race_date"].astype(np.str_),
        history_values=history_values,
        year=year,
    )
    return forecast_horse_year(queries, forecaster, fallback=fallback)


def run(args: argparse.Namespace) -> TreeFoldResult:
    if platform.system() != "Darwin" or platform.machine() != "arm64":
        raise RuntimeError("validation execution is Mac-local")
    if args.backend == "timesfm3":
        if not args.accept_non_commercial_license:
            raise ValueError("TimesFM 3.0 requires --accept-non-commercial-license")
        forecaster = TimesFm3Forecaster(
            checkpoint="google/timesfm-3.0-pytorch",
            batch_size=args.batch_size or 4,
            device=resolve_timesfm_device(None),
        )
    else:
        forecaster = Chronos2Forecaster(batch_size=args.batch_size or 256)
    source = load_source(args.input)
    calibration = forecast(source, args.year - 1, forecaster)
    calibration_source = calibration.target_indices
    temperature = fit_temperature(
        calibration.values[:, 0],
        source["race_id"][calibration_source].astype(np.str_),
        source["finish_position"][calibration_source].astype(np.int64),
    )
    target = forecast(source, args.year, forecaster)
    target_source = target.target_indices
    race_ids = source["race_id"][target_source].astype(np.str_)
    scores = target.values[:, 0]
    frame = PredictionFrame(
        race_ids=race_ids,
        race_dates=source["race_date"][target_source].astype(np.str_),
        horse_ids=source["horse_id"][target_source].astype(np.str_),
        finish_positions=source["finish_position"][target_source].astype(np.int64),
        decimal_odds=source["decimal_odds"][target_source].astype(np.float64),
        win_probabilities=scores_to_probabilities(scores, race_ids, temperature=temperature),
        ranking_scores=scores,
    )
    result = TreeFoldResult(
        model=args.backend,
        year=args.year,
        train_rows=int(np.sum(source["race_date"].astype(np.str_) < f"{args.year - 1}0101")),
        calibration_rows=len(calibration_source),
        test_rows=len(target_source),
        temperature=temperature,
        metrics=evaluate_prediction_frame(frame),
    )
    args.output.mkdir(parents=True, exist_ok=True)
    write_prediction_frame(args.output / f"{args.backend}-{args.year}.parquet", frame)
    (args.output / f"{args.backend}-{args.year}.json").write_text(
        json.dumps(
            {
                "backend": forecaster.backend,
                "checkpoint": forecaster.checkpoint,
                "history_contract": "pre-year-only; no target-year updates",
                "history_available_rate": float(np.mean(target.history_available)),
                **asdict(result),
            },
            indent=2,
        )
        + "\n"
    )
    return result


def main() -> None:
    print(json.dumps(asdict(run(parse_args())), sort_keys=True))


if __name__ == "__main__":
    main()
