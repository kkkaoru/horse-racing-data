#!/usr/bin/env python3
# pyright: reportUnknownParameterType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportMissingParameterType=false, reportMissingTypeStubs=false, reportAttributeAccessIssue=false, reportPrivateUsage=false, reportMissingTypeArgument=false, reportUnnecessaryCast=false
"""Race Set Transformer CLI for JRA finish-position prediction.

Subcommands:
  train          single train/valid split
  walk-forward   per-year time-series folds
  predict        score a parquet with an existing checkpoint

Run with:
  .venv/bin/python src/scripts/finish_position_transformer.py walk-forward \
    --parquet tmp/finish-position-features-parquet-jra-v2 \
    --train-start-date 20160101 --validation-years 2024,2025 \
    --output-report tmp/finish-position-eval/wf-jra-transformer.json \
    --output-predictions-dir tmp/finish-position-eval/predictions-v2/transformer
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import TypedDict, cast

import mlx.core as mx
import mlx.utils
import numpy as np
import polars as pl

from finish_position_lightgbm import (
    PredictionRow,
    load_dataset,
    parse_year_list,
    split_walk_forward,
    write_predictions_jsonl,
)
from finish_position_transformer.dataset import (
    NormalizationStats,
    RaceBatchArrays,
    build_race_batches,
    categorical_vocab_size,
    fit_normalization_stats,
    race_categorical_vocab_size,
    resolve_transformer_feature_columns,
)
from finish_position_transformer.model import (
    DEFAULT_DROPOUT,
    DEFAULT_EMBEDDING_DIM,
    DEFAULT_NUM_HEADS,
    DEFAULT_NUM_LAYERS,
    ModelConfig,
    RaceSetTransformer,
    default_model_config,
)
from finish_position_transformer.training import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_EARLY_STOPPING_EPOCHS,
    DEFAULT_LEARNING_RATE,
    DEFAULT_LISTNET_WEIGHT,
    DEFAULT_MAX_EPOCHS,
    DEFAULT_PAIRWISE_WEIGHT,
    DEFAULT_PLACE2_WEIGHT,
    DEFAULT_PLACE3_WEIGHT,
    DEFAULT_CONDITIONAL_PLACE2_WEIGHT,
    DEFAULT_CONDITIONAL_PLACE3_WEIGHT,
    DEFAULT_SEED,
    DEFAULT_TOP1_WEIGHT,
    DEFAULT_TOP3_WEIGHT,
    DEFAULT_WARMUP_STEPS,
    DEFAULT_WEIGHT_DECAY,
    LossWeights,
    TrainingConfig,
    predict_all_logits,
    predict_rank_scores,
    train_transformer,
)


class CheckpointPaths(TypedDict):
    model_weights: Path
    stats: Path
    config: Path


class WalkForwardFoldReport(TypedDict):
    best_epoch: int
    best_valid_ndcg_at_3: float
    elapsed_seconds: float
    fold_year: int
    train_rows: int
    valid_rows: int


class WalkForwardReport(TypedDict):
    aggregate: dict[str, float]
    folds: list[WalkForwardFoldReport]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="finish_position_transformer")
    subparsers = parser.add_subparsers(dest="command", required=True)
    _build_train_parser(subparsers)
    _build_walk_forward_parser(subparsers)
    _build_predict_parser(subparsers)
    return parser.parse_args(argv)


def _build_train_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("train")
    parser.add_argument("--train-parquet", type=Path, required=True)
    parser.add_argument("--valid-parquet", type=Path)
    parser.add_argument("--output-model-dir", type=Path, required=True)
    parser.add_argument("--output-metadata", type=Path)
    parser.add_argument("--output-predictions", type=Path)
    _add_training_hparam_args(parser)


def _build_walk_forward_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("walk-forward")
    parser.add_argument("--parquet", type=Path, required=True)
    parser.add_argument("--train-start-date", type=str, default="20160101")
    parser.add_argument("--validation-years", type=str, default="2024,2025")
    parser.add_argument("--output-report", type=Path, required=True)
    parser.add_argument("--output-predictions-dir", type=Path)
    _add_training_hparam_args(parser)


def _build_predict_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("predict")
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--input-parquet", type=Path, required=True)
    parser.add_argument("--output-predictions", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)


def _add_training_hparam_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--max-epochs", type=int, default=DEFAULT_MAX_EPOCHS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--learning-rate", type=float, default=DEFAULT_LEARNING_RATE)
    parser.add_argument("--weight-decay", type=float, default=DEFAULT_WEIGHT_DECAY)
    parser.add_argument("--warmup-steps", type=int, default=DEFAULT_WARMUP_STEPS)
    parser.add_argument(
        "--early-stopping-epochs", type=int, default=DEFAULT_EARLY_STOPPING_EPOCHS
    )
    parser.add_argument("--top1-weight", type=float, default=DEFAULT_TOP1_WEIGHT)
    parser.add_argument("--top3-weight", type=float, default=DEFAULT_TOP3_WEIGHT)
    parser.add_argument("--pairwise-weight", type=float, default=DEFAULT_PAIRWISE_WEIGHT)
    parser.add_argument("--listnet-weight", type=float, default=DEFAULT_LISTNET_WEIGHT)
    parser.add_argument("--place2-weight", type=float, default=DEFAULT_PLACE2_WEIGHT)
    parser.add_argument("--place3-weight", type=float, default=DEFAULT_PLACE3_WEIGHT)
    parser.add_argument(
        "--conditional-place2-weight",
        type=float,
        default=DEFAULT_CONDITIONAL_PLACE2_WEIGHT,
    )
    parser.add_argument(
        "--conditional-place3-weight",
        type=float,
        default=DEFAULT_CONDITIONAL_PLACE3_WEIGHT,
    )
    parser.add_argument("--embedding-dim", type=int, default=DEFAULT_EMBEDDING_DIM)
    parser.add_argument("--num-layers", type=int, default=DEFAULT_NUM_LAYERS)
    parser.add_argument("--num-heads", type=int, default=DEFAULT_NUM_HEADS)
    parser.add_argument("--dropout", type=float, default=DEFAULT_DROPOUT)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)


def training_config_from_args(args: argparse.Namespace) -> TrainingConfig:
    weights: LossWeights = {
        "top1": float(args.top1_weight),
        "top3": float(args.top3_weight),
        "pairwise": float(args.pairwise_weight),
        "listnet": float(args.listnet_weight),
        "place2": float(args.place2_weight),
        "place3": float(args.place3_weight),
        "conditional_place2": float(args.conditional_place2_weight),
        "conditional_place3": float(args.conditional_place3_weight),
    }
    return {
        "batch_size": int(args.batch_size),
        "early_stopping_epochs": int(args.early_stopping_epochs),
        "learning_rate": float(args.learning_rate),
        "loss_weights": weights,
        "max_epochs": int(args.max_epochs),
        "seed": int(args.seed),
        "warmup_steps": int(args.warmup_steps),
        "weight_decay": float(args.weight_decay),
    }


def _checkpoint_paths(model_dir: Path) -> CheckpointPaths:
    return {
        "model_weights": model_dir / "model.safetensors",
        "stats": model_dir / "stats.json",
        "config": model_dir / "config.json",
    }


def save_checkpoint(
    model: RaceSetTransformer,
    config: ModelConfig,
    stats: NormalizationStats,
    model_dir: Path,
) -> None:
    model_dir.mkdir(parents=True, exist_ok=True)
    paths = _checkpoint_paths(model_dir)
    flat_params = dict(mlx.utils.tree_flatten(model.parameters()))
    mx.save_safetensors(str(paths["model_weights"]), flat_params)
    paths["config"].write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    paths["stats"].write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")


def load_checkpoint(model_dir: Path) -> tuple[RaceSetTransformer, NormalizationStats, ModelConfig]:
    paths = _checkpoint_paths(model_dir)
    config: ModelConfig = json.loads(paths["config"].read_text(encoding="utf-8"))
    stats: NormalizationStats = json.loads(paths["stats"].read_text(encoding="utf-8"))
    model = RaceSetTransformer(config)
    raw_weights = mx.load(str(paths["model_weights"]))
    if not isinstance(raw_weights, dict):
        raise RuntimeError(f"expected safetensors checkpoint to deserialize as dict; got {type(raw_weights)!r}")
    typed_weights = cast("dict[str, mx.array]", raw_weights)
    model.update(mlx.utils.tree_unflatten(list(typed_weights.items())))
    mx.eval(model.parameters())
    return model, stats, config


def _build_model_for_dataframe(
    df: pl.DataFrame,
    embedding_dim: int = DEFAULT_EMBEDDING_DIM,
    num_layers: int = DEFAULT_NUM_LAYERS,
    num_heads: int = DEFAULT_NUM_HEADS,
    dropout: float = DEFAULT_DROPOUT,
) -> tuple[RaceSetTransformer, ModelConfig, NormalizationStats]:
    columns = resolve_transformer_feature_columns(list(df.columns))
    stats = fit_normalization_stats(df, columns)
    vocab_sizes = [categorical_vocab_size(stats, column) for column in columns.categorical]
    race_vocab_sizes = [
        race_categorical_vocab_size(stats, column) for column in stats["race_categorical_columns"]
    ]
    config = default_model_config(
        num_numeric_features=len(columns.numeric),
        categorical_vocab_sizes=vocab_sizes,
        race_categorical_vocab_sizes=race_vocab_sizes,
        embedding_dim=embedding_dim,
        num_layers=num_layers,
        num_heads=num_heads,
        dropout=dropout,
    )
    model = RaceSetTransformer(config)
    mx.eval(model.parameters())
    return model, config, stats


def predictions_from_scores(
    arrays: RaceBatchArrays, scores: np.ndarray
) -> list[PredictionRow]:
    predictions: list[PredictionRow] = []
    mask = arrays["mask"]
    umaban = arrays["umaban"]
    horse_ids = arrays["ketto_toroku_bango"]
    for race_idx, race_id in enumerate(arrays["race_ids"]):
        race_mask = mask[race_idx]
        race_scores = scores[race_idx]
        masked = np.where(race_mask, race_scores, -np.inf)
        order = np.argsort(-masked, kind="stable")
        rank = 0
        for slot in order:
            if not bool(race_mask[slot]):
                continue
            rank += 1
            predictions.append(
                {
                    "race_id": race_id,
                    "ketto_toroku_bango": horse_ids[race_idx][slot],
                    "umaban": int(umaban[race_idx][slot]),
                    "predicted_score": float(race_scores[slot]),
                    "predicted_rank": rank,
                }
            )
    return predictions


def score_dataframe(
    model: RaceSetTransformer, stats: NormalizationStats, df: pl.DataFrame, batch_size: int
) -> list[PredictionRow]:
    arrays = build_race_batches(df, stats)
    scores = predict_rank_scores(model, arrays, batch_size=batch_size)
    return predictions_from_scores(arrays, scores)


def score_dataframe_with_conditional(
    model: RaceSetTransformer, stats: NormalizationStats, df: pl.DataFrame, batch_size: int
) -> dict[str, list[PredictionRow]]:
    arrays = build_race_batches(df, stats)
    all_logits = predict_all_logits(model, arrays, batch_size=batch_size)
    return {
        "rank_score": predictions_from_scores(arrays, all_logits["rank_score"]),
        "conditional_place2": predictions_from_scores(arrays, all_logits["conditional_place2_logit"]),
        "conditional_place3": predictions_from_scores(arrays, all_logits["conditional_place3_logit"]),
    }


def write_predictions(predictions: list[PredictionRow], path: Path) -> None:
    write_predictions_jsonl(pl.DataFrame(predictions), path)


def run_train_command(args: argparse.Namespace) -> None:
    train_df = load_dataset(args.train_parquet)
    valid_df = load_dataset(args.valid_parquet) if args.valid_parquet is not None else None
    model, config, stats = _build_model_for_dataframe(
        train_df,
        embedding_dim=int(args.embedding_dim),
        num_layers=int(args.num_layers),
        num_heads=int(args.num_heads),
        dropout=float(args.dropout),
    )
    train_arrays = build_race_batches(train_df, stats)
    valid_arrays = build_race_batches(valid_df, stats) if valid_df is not None else None
    training_config = training_config_from_args(args)
    result = train_transformer(model, train_arrays, valid_arrays, training_config)
    save_checkpoint(model, config, stats, args.output_model_dir)
    if args.output_predictions is not None:
        source_df = valid_df if valid_df is not None else train_df
        predictions = score_dataframe(model, stats, source_df, training_config["batch_size"])
        write_predictions(predictions, args.output_predictions)
    if args.output_metadata is not None:
        args.output_metadata.parent.mkdir(parents=True, exist_ok=True)
        args.output_metadata.write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    print(json.dumps(result, ensure_ascii=False))


def run_walk_forward_command(args: argparse.Namespace) -> None:
    full_df = load_dataset(args.parquet)
    validation_years = parse_year_list(args.validation_years)
    training_config = training_config_from_args(args)
    folds: list[WalkForwardFoldReport] = []
    for valid_year in validation_years:
        fold = split_walk_forward(full_df, args.train_start_date, valid_year)
        train_df = fold["train_df"]
        valid_df = fold["valid_df"]
        if train_df.height == 0 or valid_df.height == 0:
            continue
        model, _config, stats = _build_model_for_dataframe(
            train_df,
            embedding_dim=int(args.embedding_dim),
            num_layers=int(args.num_layers),
            num_heads=int(args.num_heads),
            dropout=float(args.dropout),
        )
        train_arrays = build_race_batches(train_df, stats)
        valid_arrays = build_race_batches(valid_df, stats)
        result = train_transformer(model, train_arrays, valid_arrays, training_config)
        folds.append(
            {
                "best_epoch": result["best_epoch"],
                "best_valid_ndcg_at_3": result["best_valid_ndcg_at_3"],
                "elapsed_seconds": result["elapsed_seconds"],
                "fold_year": valid_year,
                "train_rows": train_df.height,
                "valid_rows": valid_df.height,
            }
        )
        if args.output_predictions_dir is not None:
            all_preds = score_dataframe_with_conditional(
                model, stats, valid_df, training_config["batch_size"],
            )
            write_predictions(
                all_preds["rank_score"],
                args.output_predictions_dir / f"{valid_year}.jsonl",
            )
            write_predictions(
                all_preds["conditional_place2"],
                args.output_predictions_dir / f"{valid_year}-cp2.jsonl",
            )
            write_predictions(
                all_preds["conditional_place3"],
                args.output_predictions_dir / f"{valid_year}-cp3.jsonl",
            )
    aggregate = {
        "fold_count": float(len(folds)),
        "valid_ndcg_at_3_mean": float(
            sum(f["best_valid_ndcg_at_3"] for f in folds) / max(len(folds), 1)
        ),
        "elapsed_seconds_total": float(sum(f["elapsed_seconds"] for f in folds)),
    }
    report: WalkForwardReport = {"aggregate": aggregate, "folds": folds}
    args.output_report.parent.mkdir(parents=True, exist_ok=True)
    args.output_report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"aggregate": aggregate, "folds": folds}, ensure_ascii=False))


def run_predict_command(args: argparse.Namespace) -> None:
    model, stats, _config = load_checkpoint(args.model_dir)
    df = load_dataset(args.input_parquet)
    predictions = score_dataframe(model, stats, df, int(args.batch_size))
    write_predictions(predictions, args.output_predictions)
    print(
        json.dumps(
            {
                "input_rows": df.height,
                "model_dir": str(args.model_dir),
                "output_predictions": str(args.output_predictions),
                "scored_rows": len(predictions),
            },
            ensure_ascii=False,
        )
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.command == "train":
        run_train_command(args)
        return
    if args.command == "walk-forward":
        run_walk_forward_command(args)
        return
    if args.command == "predict":
        run_predict_command(args)
        return
    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
