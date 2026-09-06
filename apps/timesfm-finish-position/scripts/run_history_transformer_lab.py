#!/usr/bin/env python3
"""Train the MLX per-horse history Transformer with nested walk-forward evaluation."""

from __future__ import annotations

import argparse
import json
import platform
from dataclasses import asdict
from pathlib import Path

import mlx.core as mx
import mlx.nn as nn
import mlx.optimizers as optim
import numpy as np
import pyarrow.parquet as pq

from timesfm_finish_position.data import ArrowTableLike
from timesfm_finish_position.history_transformer import (
    HistoryTransformerConfig,
    HorseHistoryTransformer,
)
from timesfm_finish_position.horse_history import HorseHistoryRows, build_horse_history_batch
from timesfm_finish_position.lab_domain import PredictionFrame
from timesfm_finish_position.lab_metrics import evaluate_prediction_frame
from timesfm_finish_position.tabular_evaluation import (
    TreeFoldResult,
    fit_temperature,
    write_prediction_frame,
)

HISTORY_COLUMNS = (
    "performance_rating",
    "speed_figure",
    "margin_seconds",
    "final_3f_rating",
    "pace_rating",
    "distance",
    "track_code",
    "going_code",
    "class_code",
    "carried_weight",
    "body_weight",
    "jockey_code",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", type=Path, default=Path("tmp/nar-horse-history-2020-2026.parquet")
    )
    parser.add_argument("--output", type=Path, default=Path("tmp/history-transformer-lab"))
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=1024)
    return parser.parse_args()


def numeric_column(table: ArrowTableLike, name: str) -> np.ndarray:
    column = table.column(name)
    values = column.to_pylist()
    converted = []
    for value in values:
        if not isinstance(value, (str, int, float)):
            converted.append(0.0)
            continue
        try:
            converted.append(float(value))
        except ValueError:
            converted.append(0.0)
    return np.asarray(converted, dtype=np.float64)


def load_rows(path: Path) -> tuple[HorseHistoryRows, dict[str, np.ndarray]]:
    table = pq.read_table(path)
    values = np.column_stack([numeric_column(table, name) for name in HISTORY_COLUMNS])
    values[:, 5] /= 3200.0
    values[:, 6] /= 99.0
    values[:, 7] /= 9.0
    values[:, 8] /= 999.0
    values[:, 9] /= 100.0
    values[:, 10] /= 1000.0
    values[:, 11] /= 10000.0
    metadata = {
        name: np.asarray(table.column(name).to_pylist())
        for name in (
            "race_id",
            "race_date",
            "horse_id",
            "finish_position",
            "decimal_odds",
            "performance_rating",
        )
    }
    rows = HorseHistoryRows(
        horse_ids=metadata["horse_id"].astype(np.str_),
        race_dates=metadata["race_date"].astype(np.str_),
        values_without_interval=values,
    )
    return rows, metadata


def standardize(
    values: np.ndarray, mask: np.ndarray, train: np.ndarray
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    observed = values[train][mask[train]]
    means = np.mean(observed, axis=0)
    scales = np.std(observed, axis=0)
    scales[scales < 1e-8] = 1.0
    normalized = (values - means[None, None, :]) / scales[None, None, :]
    return np.where(mask[:, :, None], normalized, 0.0).astype(np.float32), means, scales


def predict(
    model: HorseHistoryTransformer,
    values: np.ndarray,
    mask: np.ndarray,
    gap: np.ndarray,
    indices: np.ndarray,
    batch_size: int,
) -> np.ndarray:
    model.eval()
    outputs: list[np.ndarray] = []
    for start in range(0, len(indices), batch_size):
        selected = indices[start : start + batch_size]
        _, performance, _ = model(
            mx.array(values[selected]),
            mx.array(mask[selected]),
            mx.array(gap[selected].astype(np.float32) / 365.0),
        )
        mx.eval(performance)
        outputs.append(np.asarray(performance, dtype=np.float64))
    return np.concatenate(outputs)


def run(args: argparse.Namespace) -> TreeFoldResult:
    if platform.system() != "Darwin" or platform.machine() != "arm64":
        raise RuntimeError(
            "this validation command is intentionally Mac-local; model code remains portable"
        )
    rows, metadata = load_rows(args.input)
    years = np.asarray([value[:4] for value in rows.race_dates], dtype=np.str_)
    selected_source = years <= str(args.year)
    batch = build_horse_history_batch(rows, selected_source, max_history=10)
    target_years = years[batch.target_indices]
    train = target_years < str(args.year - 1)
    calibration = target_years == str(args.year - 1)
    test = target_years == str(args.year)
    if not np.any(train) or not np.any(calibration) or not np.any(test):
        raise ValueError("history Transformer nested partition is empty")
    values, _, _ = standardize(batch.values, batch.mask, train)
    targets = metadata["performance_rating"][batch.target_indices].astype(np.float32)
    config = HistoryTransformerConfig(input_features=values.shape[2])
    model = HorseHistoryTransformer(config)
    mx.eval(model.parameters())
    optimizer = optim.AdamW(learning_rate=3e-4, weight_decay=1e-4)

    def loss_function(
        active_model: HorseHistoryTransformer,
        history: mx.array,
        history_mask: mx.array,
        gap: mx.array,
        target: mx.array,
    ) -> mx.array:
        _, prediction, uncertainty = active_model(history, history_mask, gap)
        variance = uncertainty**2 + 1e-4
        return mx.mean((prediction - target) ** 2 / variance + mx.log(variance))

    loss_and_grad = nn.value_and_grad(model, loss_function)
    train_indices = np.flatnonzero(train)
    rng = np.random.default_rng(20260902)
    model.train()
    for epoch in range(args.epochs):
        shuffled = rng.permutation(train_indices)
        losses = []
        for start in range(0, len(shuffled), args.batch_size):
            indices = shuffled[start : start + args.batch_size]
            loss, gradients = loss_and_grad(
                model,
                mx.array(values[indices]),
                mx.array(batch.mask[indices]),
                mx.array(batch.target_days_since_last[indices].astype(np.float32) / 365.0),
                mx.array(targets[indices]),
            )
            optimizer.update(model, gradients)
            mx.eval(model.parameters(), optimizer.state, loss)
            losses.append(float(loss.item()))
        print(json.dumps({"epoch": epoch + 1, "loss": float(np.mean(losses))}), flush=True)
    calibration_indices = np.flatnonzero(calibration)
    calibration_scores = predict(
        model,
        values,
        batch.mask,
        batch.target_days_since_last,
        calibration_indices,
        args.batch_size,
    )
    source_calibration = batch.target_indices[calibration_indices]
    temperature = fit_temperature(
        calibration_scores,
        metadata["race_id"][source_calibration].astype(np.str_),
        metadata["finish_position"][source_calibration].astype(np.int64),
    )
    test_indices = np.flatnonzero(test)
    test_scores = predict(
        model,
        values,
        batch.mask,
        batch.target_days_since_last,
        test_indices,
        args.batch_size,
    )
    source_test = batch.target_indices[test_indices]
    race_ids = metadata["race_id"][source_test].astype(np.str_)
    from timesfm_finish_position.tabular_evaluation import scores_to_probabilities

    frame = PredictionFrame(
        race_ids=race_ids,
        race_dates=metadata["race_date"][source_test].astype(np.str_),
        horse_ids=metadata["horse_id"][source_test].astype(np.str_),
        finish_positions=metadata["finish_position"][source_test].astype(np.int64),
        decimal_odds=metadata["decimal_odds"][source_test].astype(np.float64),
        win_probabilities=scores_to_probabilities(test_scores, race_ids, temperature=temperature),
        ranking_scores=test_scores,
    )
    result = TreeFoldResult(
        model="horse-history-transformer",
        year=args.year,
        train_rows=int(np.sum(train)),
        calibration_rows=int(np.sum(calibration)),
        test_rows=int(np.sum(test)),
        temperature=temperature,
        metrics=evaluate_prediction_frame(frame),
    )
    args.output.mkdir(parents=True, exist_ok=True)
    write_prediction_frame(args.output / f"horse-history-transformer-{args.year}.parquet", frame)
    (args.output / f"horse-history-transformer-{args.year}.json").write_text(
        json.dumps({"backend": "mlx-metal", **asdict(result)}, indent=2) + "\n"
    )
    return result


def main() -> None:
    print(json.dumps(asdict(run(parse_args())), sort_keys=True))


if __name__ == "__main__":
    main()
