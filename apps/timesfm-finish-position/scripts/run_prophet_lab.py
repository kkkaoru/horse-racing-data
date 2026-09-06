#!/usr/bin/env python3
"""Evaluate PIT-safe venue, jockey, and trainer Prophet trend features."""

from __future__ import annotations

import argparse
import json
import logging
import platform
from dataclasses import asdict
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from prophet import Prophet

from timesfm_finish_position.lab_domain import PredictionFrame
from timesfm_finish_position.lab_metrics import evaluate_prediction_frame
from timesfm_finish_position.prophet_features import build_entity_trend_features
from timesfm_finish_position.tabular_evaluation import (
    TreeFoldResult,
    fit_temperature,
    scores_to_probabilities,
    write_prediction_frame,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", type=Path, default=Path("tmp/nar-horse-history-2020-2026.parquet")
    )
    parser.add_argument("--output", type=Path, default=Path("tmp/prophet-lab"))
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--max-entities", type=int, default=32)
    return parser.parse_args()


def prophet_forecast(dates: np.ndarray, values: np.ndarray, target_dates: np.ndarray) -> np.ndarray:
    model = Prophet(
        yearly_seasonality=False,
        weekly_seasonality=False,
        daily_seasonality=False,
        n_changepoints=5,
        changepoint_prior_scale=0.05,
        uncertainty_samples=0,
    )
    model.fit(pd.DataFrame({"ds": pd.to_datetime(dates), "y": values}), algorithm="LBFGS")
    unique, inverse = np.unique(target_dates, return_inverse=True)
    prediction = model.predict(pd.DataFrame({"ds": pd.to_datetime(unique)}))["yhat"].to_numpy()
    return np.asarray(prediction[inverse], dtype=np.float64)


def load_source(path: Path) -> dict[str, np.ndarray]:
    names = [
        "race_id",
        "race_date",
        "venue_code",
        "horse_id",
        "jockey_code",
        "trainer_code",
        "finish_position",
        "decimal_odds",
        "performance_rating",
    ]
    table = pq.read_table(path, columns=names)
    return {name: np.asarray(table.column(name).to_pylist()) for name in names}


def features(source: dict[str, np.ndarray], year: int, max_entities: int):
    return build_entity_trend_features(
        race_dates=source["race_date"].astype(np.str_),
        entity_columns=tuple(
            source[name].astype(np.str_) for name in ("venue_code", "jockey_code", "trainer_code")
        ),
        performance=source["performance_rating"].astype(np.float64),
        year=year,
        forecaster=prophet_forecast,
        max_entities=max_entities,
        minimum_history_rows=100,
    )


def run(args: argparse.Namespace) -> TreeFoldResult:
    if platform.system() != "Darwin" or platform.machine() != "arm64":
        raise RuntimeError("validation execution is Mac-local")
    logging.getLogger("cmdstanpy").setLevel(logging.WARNING)
    source = load_source(args.input)
    calibration = features(source, args.year - 1, args.max_entities)
    calibration_scores = np.mean(calibration.values, axis=1)
    calibration_source = calibration.target_indices
    temperature = fit_temperature(
        calibration_scores,
        source["race_id"][calibration_source].astype(np.str_),
        source["finish_position"][calibration_source].astype(np.int64),
    )
    target = features(source, args.year, args.max_entities)
    target_source = target.target_indices
    scores = np.mean(target.values, axis=1)
    race_ids = source["race_id"][target_source].astype(np.str_)
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
        model="prophet-entity-trends",
        year=args.year,
        train_rows=int(np.sum(source["race_date"].astype(np.str_) < f"{args.year - 1}0101")),
        calibration_rows=len(calibration_source),
        test_rows=len(target_source),
        temperature=temperature,
        metrics=evaluate_prediction_frame(frame),
    )
    args.output.mkdir(parents=True, exist_ok=True)
    write_prediction_frame(args.output / f"prophet-entity-trends-{args.year}.parquet", frame)
    (args.output / f"prophet-entity-trends-{args.year}.json").write_text(
        json.dumps(
            {
                "feature_contract": "venue+jockey+trainer monthly Prophet; pre-year-only",
                "selected_entities": target.selected_entities,
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
