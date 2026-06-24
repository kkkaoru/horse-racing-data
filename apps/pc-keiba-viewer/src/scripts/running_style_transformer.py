#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportUnannotatedClassAttribute=false, reportImplicitOverride=false
"""Minimal MLX Race Set Transformer for running-style 4-class softmax.

Reads the parquet produced by finish_position_features_duckdb.py, groups
runners by race, and trains a transformer that attends across runners
within a race. Single softmax head emits 4-class probabilities matching
the LightGBM running-style output schema, so the produced JSONL is a
drop-in replacement for import-running-style-predictions.ts.

Run with:
  cd src/scripts && ../../.venv/bin/python -m running_style_transformer \\
    --csv ../../tmp/feat-jra-v5-post \\
    --train-start-date 20160101 \\
    --validation-years 2024,2025 \\
    --output-predictions-dir ../../tmp/eval-rs-jra-trans
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from time import perf_counter
from typing import TypedDict, cast

import mlx.core as mx
import mlx.nn as nn  # pyright: ignore[reportMissingTypeStubs]
import mlx.optimizers as opt  # pyright: ignore[reportMissingTypeStubs]
import numpy as np
import polars as pl

from finish_position_transformer.dataset import (
    MAX_RUNNERS,
    NormalizationStats,
    RaceBatchArrays,
    build_race_batches,
    categorical_vocab_size,
    fit_normalization_stats,
    race_categorical_vocab_size,
    resolve_transformer_feature_columns,
)
from finish_position_transformer.dataset import FeatureColumns
from running_style_lightgbm import (
    CLASS_LABELS,
    LABEL_COLUMNS as RUNNING_STYLE_LABEL_COLUMNS,
    NUM_CLASSES,
    PROBABILITY_COLUMNS,
    TARGET_COLUMN,
)


def resolve_running_style_feature_columns(df_columns: list[str]) -> FeatureColumns:
    """Drop running-style target columns from features to prevent leak."""
    base = resolve_transformer_feature_columns(df_columns)
    excluded = set(RUNNING_STYLE_LABEL_COLUMNS)
    numeric = [c for c in base.numeric if c not in excluded]
    categorical = [c for c in base.categorical if c not in excluded]
    return FeatureColumns(numeric=numeric, categorical=categorical)

DEFAULT_EMBEDDING_DIM = 64
DEFAULT_NUM_LAYERS = 2
DEFAULT_NUM_HEADS = 4
DEFAULT_FFN_DIM_MULTIPLIER = 4
DEFAULT_DROPOUT = 0.1
DEFAULT_BATCH_SIZE = 64
DEFAULT_LEARNING_RATE = 1e-3
DEFAULT_WEIGHT_DECAY = 1e-4
DEFAULT_MAX_EPOCHS = 20
DEFAULT_EARLY_STOPPING_EPOCHS = 4
PAD_TARGET_CLASS = -1
ATTENTION_NEG_INF = -1e9
UMABAN_VOCAB_SIZE = MAX_RUNNERS + 1


class TrainingParams(TypedDict):
    embedding_dim: int
    num_layers: int
    num_heads: int
    dropout: float
    batch_size: int
    learning_rate: float
    weight_decay: float
    max_epochs: int
    early_stopping_epochs: int
    seed: int


class FoldMetrics(TypedDict):
    validation_year: int
    train_rows: int
    valid_rows: int
    accuracy: float
    macro_f1: float
    best_epoch: int


def default_training_params() -> TrainingParams:
    return {
        "embedding_dim": DEFAULT_EMBEDDING_DIM,
        "num_layers": DEFAULT_NUM_LAYERS,
        "num_heads": DEFAULT_NUM_HEADS,
        "dropout": DEFAULT_DROPOUT,
        "batch_size": DEFAULT_BATCH_SIZE,
        "learning_rate": DEFAULT_LEARNING_RATE,
        "weight_decay": DEFAULT_WEIGHT_DECAY,
        "max_epochs": DEFAULT_MAX_EPOCHS,
        "early_stopping_epochs": DEFAULT_EARLY_STOPPING_EPOCHS,
        "seed": 20260518,
    }


def _read_partitioned_parquet(child: Path) -> pl.DataFrame:
    frame = pl.read_parquet(child)
    if "race_year" not in frame.columns:
        year_token = child.parent.name
        if year_token.startswith("race_year="):
            frame = frame.with_columns(
                pl.lit(int(year_token.split("=", 1)[1])).alias("race_year")
            )
    return frame


def load_dataset_parquet(path: Path) -> pl.DataFrame:
    if path.is_dir():
        partitioned = sorted(path.glob("race_year=*/*.parquet"))
        if partitioned:
            return pl.concat(
                [_read_partitioned_parquet(child) for child in partitioned], how="diagonal_relaxed"
            )
        flat = sorted(path.glob("*.parquet"))
        if flat:
            return pl.concat(
                [pl.read_parquet(child) for child in flat], how="diagonal_relaxed"
            )
        raise ValueError(f"No parquet files found under {path}")
    return pl.read_parquet(path)


def split_by_year(df: pl.DataFrame, train_start: str, valid_year: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    race_date = pl.col("race_date").str.to_datetime("%Y%m%d")
    train_mask = (race_date >= pl.lit(train_start).str.to_datetime("%Y%m%d")) & (
        pl.col("race_year") < valid_year
    )
    valid_mask = pl.col("race_year") == valid_year
    return df.filter(train_mask), df.filter(valid_mask)


def extract_running_style_targets(df: pl.DataFrame, race_ids: list[str]) -> np.ndarray:
    by_race_dict: dict[str, pl.DataFrame] = {
        str(race_id): df_part
        for (race_id,), df_part in df.sort(["race_id", "umaban"]).group_by(
            ["race_id"], maintain_order=True
        )
    }
    num_races = len(race_ids)
    targets = np.full((num_races, MAX_RUNNERS), PAD_TARGET_CLASS, dtype=np.int32)
    for race_idx, race_id in enumerate(race_ids):
        group = by_race_dict.get(race_id)
        if group is None:
            continue
        for slot, raw in enumerate(group[TARGET_COLUMN].to_list()):
            if slot >= MAX_RUNNERS:
                break
            if raw is None:
                continue
            targets[race_idx, slot] = int(raw)
    return targets


def build_padding_mask(mask: mx.array, num_heads: int) -> mx.array:
    del num_heads
    keep = mask.astype(mx.float32)
    additive = (1.0 - keep) * ATTENTION_NEG_INF
    return additive[:, None, None, :]


class RunningStyleTransformer(nn.Module):
    def __init__(
        self,
        num_numeric_features: int,
        categorical_vocab_sizes: list[int],
        race_categorical_vocab_sizes: list[int],
        embedding_dim: int,
        num_layers: int,
        num_heads: int,
        dropout: float,
    ) -> None:
        super().__init__()
        self.numeric_projection = nn.Linear(num_numeric_features, embedding_dim)
        self.categorical_embeddings = [
            nn.Embedding(num_embeddings=vocab, dims=embedding_dim) for vocab in categorical_vocab_sizes
        ]
        self.race_categorical_embeddings = [
            nn.Embedding(num_embeddings=vocab, dims=embedding_dim)
            for vocab in race_categorical_vocab_sizes
        ]
        self.umaban_embedding = nn.Embedding(num_embeddings=UMABAN_VOCAB_SIZE, dims=embedding_dim)
        self.input_layer_norm = nn.LayerNorm(dims=embedding_dim)
        self.encoder = nn.TransformerEncoder(
            num_layers=num_layers,
            dims=embedding_dim,
            num_heads=num_heads,
            mlp_dims=embedding_dim * DEFAULT_FFN_DIM_MULTIPLIER,
            dropout=dropout,
            norm_first=True,
        )
        self.head = nn.Linear(embedding_dim, NUM_CLASSES)
        self.num_heads = num_heads

    def __call__(
        self,
        numeric_features: mx.array,
        categorical_indices: mx.array,
        race_categorical_indices: mx.array,
        umaban: mx.array,
        mask: mx.array,
    ) -> mx.array:
        x = self.numeric_projection(numeric_features)
        for col_idx, embedding in enumerate(self.categorical_embeddings):
            x = x + embedding(categorical_indices[:, :, col_idx])
        for col_idx, embedding in enumerate(self.race_categorical_embeddings):
            race_embedding = embedding(race_categorical_indices[:, col_idx])
            x = x + race_embedding[:, None, :]
        x = x + self.umaban_embedding(umaban)
        x = self.input_layer_norm(x)
        attention_mask = build_padding_mask(mask, self.num_heads)
        encoded = self.encoder(x, attention_mask)
        return self.head(encoded)


def to_mx_batch(arrays: dict[str, np.ndarray], targets: np.ndarray) -> dict[str, mx.array]:
    return {
        "numeric_features": mx.array(arrays["numeric_features"]),
        "categorical_indices": mx.array(arrays["categorical_indices"]),
        "race_categorical_indices": mx.array(arrays["race_categorical_indices"]),
        "umaban": mx.array(arrays["umaban"]),
        "mask": mx.array(arrays["mask"]),
        "targets": mx.array(targets),
    }


def slice_arrays(arrays: dict[str, np.ndarray], indices: np.ndarray) -> dict[str, np.ndarray]:
    return {key: value[indices] for key, value in arrays.items() if hasattr(value, "shape")}


def cross_entropy_with_mask(
    logits: mx.array, targets: mx.array, mask: mx.array
) -> mx.array:
    valid = mx.logical_and(mask, mx.not_equal(targets, PAD_TARGET_CLASS))
    safe_targets = mx.where(valid, targets, mx.zeros_like(targets))
    losses = nn.losses.cross_entropy(logits, safe_targets, reduction="none")
    masked_losses = mx.where(valid, losses, mx.zeros_like(losses))
    denom = mx.maximum(mx.sum(valid.astype(mx.float32)), mx.array(1.0))
    return mx.sum(masked_losses) / denom


def _build_batched_inputs(arrays: RaceBatchArrays, targets: np.ndarray) -> dict[str, np.ndarray]:
    return {
        "numeric_features": arrays["numeric_features"],
        "categorical_indices": arrays["categorical_indices"],
        "race_categorical_indices": arrays["race_categorical_indices"],
        "umaban": arrays["umaban"],
        "mask": arrays["mask"],
        "targets": targets,
    }


def iterate_batches(
    batched: dict[str, np.ndarray], batch_size: int, shuffle: bool, rng: np.random.Generator
) -> list[dict[str, mx.array]]:
    num_races = batched["numeric_features"].shape[0]
    indices = np.arange(num_races)
    if shuffle:
        rng.shuffle(indices)
    out: list[dict[str, mx.array]] = []
    for start in range(0, num_races, batch_size):
        batch_idx = indices[start : start + batch_size]
        out.append(
            to_mx_batch(
                {key: batched[key][batch_idx] for key in (
                    "numeric_features", "categorical_indices", "race_categorical_indices", "umaban", "mask"
                )},
                batched["targets"][batch_idx],
            )
        )
    return out


def compute_softmax_predictions(
    model: RunningStyleTransformer, batches: list[dict[str, mx.array]]
) -> np.ndarray:
    chunks: list[np.ndarray] = []
    for batch in batches:
        logits = model(
            batch["numeric_features"],
            batch["categorical_indices"],
            batch["race_categorical_indices"],
            batch["umaban"],
            batch["mask"],
        )
        probabilities = mx.softmax(logits, axis=-1)
        mx.eval(probabilities)
        chunks.append(np.array(probabilities))
    return np.concatenate(chunks, axis=0)


def evaluate_validation_loss(
    model: RunningStyleTransformer, valid_batches: list[dict[str, mx.array]]
) -> float:
    total_loss = 0.0
    count = 0
    for batch in valid_batches:
        logits = model(
            batch["numeric_features"],
            batch["categorical_indices"],
            batch["race_categorical_indices"],
            batch["umaban"],
            batch["mask"],
        )
        loss = cross_entropy_with_mask(logits, batch["targets"], batch["mask"])
        mx.eval(loss)
        total_loss += float(loss.item())
        count += 1
    return total_loss / max(count, 1)


def train_one_fold(
    train_df: pl.DataFrame,
    valid_df: pl.DataFrame,
    params: TrainingParams,
) -> tuple[RunningStyleTransformer, NormalizationStats, np.ndarray, np.ndarray, RaceBatchArrays, int]:
    feature_columns = resolve_running_style_feature_columns(list(train_df.columns))
    stats = fit_normalization_stats(train_df, feature_columns)
    train_arrays = build_race_batches(train_df, stats)
    valid_arrays = build_race_batches(valid_df, stats)
    train_targets = extract_running_style_targets(train_df, train_arrays["race_ids"])
    valid_targets = extract_running_style_targets(valid_df, valid_arrays["race_ids"])
    categorical_vocab_sizes = [
        categorical_vocab_size(stats, column) for column in stats["categorical_columns"]
    ]
    race_categorical_vocab_sizes = [
        race_categorical_vocab_size(stats, column) for column in stats["race_categorical_columns"]
    ]
    model = RunningStyleTransformer(
        num_numeric_features=len(stats["numeric_columns"]),
        categorical_vocab_sizes=categorical_vocab_sizes,
        race_categorical_vocab_sizes=race_categorical_vocab_sizes,
        embedding_dim=params["embedding_dim"],
        num_layers=params["num_layers"],
        num_heads=params["num_heads"],
        dropout=params["dropout"],
    )
    optimizer = opt.AdamW(learning_rate=params["learning_rate"], weight_decay=params["weight_decay"])
    rng = np.random.default_rng(params["seed"])
    train_batched = _build_batched_inputs(train_arrays, train_targets)
    valid_batched = _build_batched_inputs(valid_arrays, valid_targets)
    best_loss = float("inf")
    best_epoch = 0
    stale_epochs = 0
    loss_and_grad = nn.value_and_grad(model, cross_entropy_with_mask_for_model)
    for epoch in range(1, params["max_epochs"] + 1):
        model.train()
        for batch in iterate_batches(train_batched, params["batch_size"], shuffle=True, rng=rng):
            loss_value, grads = loss_and_grad(
                model,
                batch["numeric_features"],
                batch["categorical_indices"],
                batch["race_categorical_indices"],
                batch["umaban"],
                batch["mask"],
                batch["targets"],
            )
            optimizer.update(model, grads)
            mx.eval(model.parameters(), optimizer.state, loss_value)
        model.eval()
        valid_batches = iterate_batches(valid_batched, params["batch_size"], shuffle=False, rng=rng)
        valid_loss = evaluate_validation_loss(model, valid_batches)
        print(json.dumps({"epoch": epoch, "valid_loss": valid_loss}))
        if valid_loss < best_loss - 1e-4:
            best_loss = valid_loss
            best_epoch = epoch
            stale_epochs = 0
        else:
            stale_epochs += 1
            if stale_epochs >= params["early_stopping_epochs"]:
                break
    valid_batches = iterate_batches(valid_batched, params["batch_size"], shuffle=False, rng=rng)
    probabilities = compute_softmax_predictions(model, valid_batches)
    return model, stats, probabilities, valid_targets, valid_arrays, best_epoch


def cross_entropy_with_mask_for_model(
    model: RunningStyleTransformer,
    numeric_features: mx.array,
    categorical_indices: mx.array,
    race_categorical_indices: mx.array,
    umaban: mx.array,
    mask: mx.array,
    targets: mx.array,
) -> mx.array:
    logits = model(numeric_features, categorical_indices, race_categorical_indices, umaban, mask)
    return cross_entropy_with_mask(logits, targets, mask)


def _sanitize_float(value: float | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return float(value)


def _build_valid_lookup(
    valid_df: pl.DataFrame,
) -> dict[tuple[str, str], tuple[object, object, object]]:
    lookup: dict[tuple[str, str], tuple[object, object, object]] = {}
    for record in valid_df.select(
        ["race_id", "ketto_toroku_bango", TARGET_COLUMN, "umaban", "race_year"]
    ).iter_rows(named=True):
        key = (str(record["race_id"]), str(record["ketto_toroku_bango"]))
        if key in lookup:
            continue
        lookup[key] = (record[TARGET_COLUMN], record["umaban"], record["race_year"])
    return lookup


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return int(cast(float, value))


def build_predictions_df(
    valid_df: pl.DataFrame,
    valid_arrays: RaceBatchArrays,
    probabilities: np.ndarray,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    race_ids = valid_arrays["race_ids"]
    mask = valid_arrays["mask"]
    ketto = valid_arrays["ketto_toroku_bango"]
    valid_lookup = _build_valid_lookup(valid_df)
    for race_idx, race_id in enumerate(race_ids):
        for slot in range(MAX_RUNNERS):
            if not mask[race_idx, slot]:
                continue
            horse_id = ketto[race_idx][slot]
            if horse_id == "":
                continue
            probs = probabilities[race_idx, slot]
            predicted_class = int(np.argmax(probs))
            found = valid_lookup.get((str(race_id), str(horse_id)))
            if found is None:
                continue
            target_raw, umaban_raw, race_year_raw = found
            rows.append(
                {
                    "race_id": race_id,
                    "ketto_toroku_bango": horse_id,
                    "umaban": _optional_int(umaban_raw),
                    "race_year": _optional_int(race_year_raw),
                    TARGET_COLUMN: _optional_int(target_raw),
                    PROBABILITY_COLUMNS[0]: _sanitize_float(float(probs[0])),
                    PROBABILITY_COLUMNS[1]: _sanitize_float(float(probs[1])),
                    PROBABILITY_COLUMNS[2]: _sanitize_float(float(probs[2])),
                    PROBABILITY_COLUMNS[3]: _sanitize_float(float(probs[3])),
                    "predicted_label": CLASS_LABELS[predicted_class],
                    "predicted_class": predicted_class,
                }
            )
    return pl.DataFrame(rows)


def compute_accuracy(predictions: pl.DataFrame) -> float:
    labeled = predictions.drop_nulls(subset=[TARGET_COLUMN])
    if len(labeled) == 0:
        return float("nan")
    target = labeled[TARGET_COLUMN].cast(pl.Int64)
    predicted = labeled["predicted_class"].cast(pl.Int64)
    correct = int((target == predicted).sum())
    return float(correct / len(labeled))


def compute_macro_f1(predictions: pl.DataFrame) -> float:
    labeled = predictions.drop_nulls(subset=[TARGET_COLUMN])
    if len(labeled) == 0:
        return float("nan")
    target = labeled[TARGET_COLUMN].cast(pl.Int64)
    predicted = labeled["predicted_class"].cast(pl.Int64)
    f1_values: list[float] = []
    for class_idx in range(NUM_CLASSES):
        actual_mask = target == class_idx
        predicted_mask = predicted == class_idx
        tp = int((actual_mask & predicted_mask).sum())
        predicted_count = int(predicted_mask.sum())
        actual_count = int(actual_mask.sum())
        if predicted_count == 0 or actual_count == 0 or tp == 0:
            continue
        precision = tp / predicted_count
        recall = tp / actual_count
        f1_values.append(2.0 * precision * recall / (precision + recall))
    return float(np.mean(f1_values)) if f1_values else float("nan")


def write_predictions_jsonl(predictions: pl.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for record in predictions.iter_rows(named=True):
            sanitized: dict[str, object] = {}
            for key, value in record.items():
                key_str = str(key)
                if isinstance(value, float) and not np.isfinite(value):
                    sanitized[key_str] = None
                else:
                    sanitized[key_str] = value
            handle.write(json.dumps(sanitized, ensure_ascii=False) + "\n")


def write_walk_forward_report(folds: list[FoldMetrics], output_path: Path) -> None:
    aggregate = {
        "accuracy_mean": float(np.nanmean([fold["accuracy"] for fold in folds])),
        "macro_f1_mean": float(np.nanmean([fold["macro_f1"] for fold in folds])),
    }
    payload = {"folds": folds, "aggregate": aggregate}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="running_style_transformer")
    subparsers = parser.add_subparsers(dest="command", required=True)
    walk = subparsers.add_parser("walk-forward")
    walk.add_argument("--csv", type=Path, required=True)
    walk.add_argument("--train-start-date", type=str, default="20160101")
    walk.add_argument("--validation-years", type=str, default="2024,2025")
    walk.add_argument("--output-predictions-dir", type=Path, required=True)
    walk.add_argument("--output-report", type=Path, default=None)
    walk.add_argument("--max-epochs", type=int, default=DEFAULT_MAX_EPOCHS)
    walk.add_argument("--early-stopping-epochs", type=int, default=DEFAULT_EARLY_STOPPING_EPOCHS)
    walk.add_argument("--seed", type=int, default=20260518)
    return parser.parse_args(argv)


def parse_validation_years(value: str) -> list[int]:
    return [int(token.strip()) for token in value.split(",") if token.strip()]


def run_walk_forward(args: argparse.Namespace) -> None:
    started = perf_counter()
    df = load_dataset_parquet(args.csv)
    params = default_training_params()
    params["max_epochs"] = args.max_epochs
    params["early_stopping_epochs"] = args.early_stopping_epochs
    params["seed"] = args.seed
    validation_years = parse_validation_years(args.validation_years)
    folds: list[FoldMetrics] = []
    all_predictions: list[pl.DataFrame] = []
    for valid_year in validation_years:
        train_df, valid_df = split_by_year(df, args.train_start_date, valid_year)
        fold_started = perf_counter()
        _model, _stats, probabilities, _valid_targets, valid_arrays, best_epoch = train_one_fold(
            train_df, valid_df, params
        )
        predictions_df = build_predictions_df(valid_df, valid_arrays, probabilities)
        accuracy = compute_accuracy(predictions_df)
        macro_f1 = compute_macro_f1(predictions_df)
        fold_metrics: FoldMetrics = {
            "validation_year": valid_year,
            "train_rows": int(len(train_df)),
            "valid_rows": int(len(valid_df)),
            "accuracy": accuracy,
            "macro_f1": macro_f1,
            "best_epoch": best_epoch,
        }
        folds.append(fold_metrics)
        all_predictions.append(predictions_df)
        print(json.dumps({
            "fold": fold_metrics,
            "fold_elapsed_seconds": perf_counter() - fold_started,
        }))
    combined = pl.concat(all_predictions, how="diagonal_relaxed")
    range_label = f"{validation_years[0]}-{validation_years[-1]}"
    output_jsonl = args.output_predictions_dir / f"{range_label}.jsonl"
    write_predictions_jsonl(combined, output_jsonl)
    if args.output_report is not None:
        write_walk_forward_report(folds, args.output_report)
    elapsed = perf_counter() - started
    print(json.dumps({"elapsed_seconds": elapsed, "predictions_jsonl": str(output_jsonl), "rows": len(combined)}))


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.command == "walk-forward":
        run_walk_forward(args)


if __name__ == "__main__":
    main()
