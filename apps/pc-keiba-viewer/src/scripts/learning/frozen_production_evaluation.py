"""Frozen CatBoost research replay with explicit dataframe missingness semantics."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from catboost import CatBoost
from predict_lib.scorer import build_feature_matrix

from learning.history_ablation import exact_rank_metrics


def score_frozen_catboost(
    frame: pl.DataFrame,
    model: CatBoost,
    *,
    training_end: str,
    feature_names: tuple[str, ...] | None = None,
    observed_only: bool = True,
    frame_loader: str = "polars",
) -> pl.DataFrame:
    """Score finishers or explicitly requested entrants without invented labels.

    The legacy Polars path passes null as None (then the scorer coerces it to
    zero). The explicit pandas path converts numeric nulls to NaN, matching the
    production dataframe ingestion step before that same scorer. Neither path
    alone proves feature/source/market parity or applies Prophet postprocessing.
    """
    if frame_loader not in ("polars", "pandas"):
        raise ValueError("Unsupported frame loader")
    if date.fromisoformat(training_end).strftime("%Y%m%d") != training_end:
        raise ValueError("Training end must use YYYYMMDD format")
    native_names = model.feature_names_
    names = native_names if feature_names is None else list(feature_names)
    if not names or len(set(names)) != len(names):
        raise ValueError("Model requires nonempty, unique feature names")
    positional_names = [str(index) for index in range(len(names))]
    if native_names != names and native_names != positional_names:
        raise ValueError("Metadata and model predictor order differ")
    missing = set(names).difference(frame.columns)
    if missing:
        raise ValueError(f"Missing model predictors: {sorted(missing)}")
    observed = frame.filter(pl.col("finish_position") > 0) if observed_only else frame
    if observed.is_empty():
        raise ValueError(
            "No observed finishers to evaluate"
            if observed_only
            else "No runners to score"
        )
    dates = observed["race_date"]
    if dates.null_count() or not isinstance(dates.min(), str):
        raise ValueError("Evaluation dates must be complete YYYYMMDD strings")
    if observed.filter(pl.col("race_date") <= training_end).height:
        raise ValueError("Frozen-model evaluation must strictly follow training")
    normalized = observed.with_columns(
        pl.concat_str(
            "source", "race_date", "keibajo_code", "race_bango", separator="-"
        ).alias("race_id"),
        pl.col("ketto_toroku_bango").alias("horse_id"),
        pl.col("umaban").alias("horse_number"),
        pl.col("finish_position").alias("finish"),
    ).sort("race_id", "horse_number")
    if normalized.select("race_id", "horse_number").is_duplicated().any():
        raise ValueError("Duplicate race/horse observations")
    selected = normalized.select(names)
    entries = selected.to_dicts()
    if frame_loader == "pandas":
        entries = [
            dict(zip(names, row, strict=True))
            for row in selected.to_pandas().itertuples(index=False, name=None)
        ]
    matrix = build_feature_matrix(entries, names, "catboost")
    scores = np.asarray(model.predict(matrix, prediction_type="RawFormulaVal"))
    if scores.shape != (normalized.height,) or not np.isfinite(scores).all():
        raise ValueError("Model must return one finite score per runner")
    return (
        normalized.select("race_id", "race_date", "horse_id", "horse_number", "finish")
        .with_columns(
            pl.Series("score", scores),
        )
        .with_columns(
            pl.col("score")
            .rank(method="ordinal", descending=True)
            .over("race_id")
            .alias("predicted_rank")
        )
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--features", required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--include-unlabelled", action="store_true")
    parser.add_argument(
        "--frame-loader", choices=("polars", "pandas"), default="polars"
    )
    args = parser.parse_args(argv)
    metadata = json.loads(
        (args.model_dir / "metadata.json").read_text(encoding="utf-8")
    )
    training_end = metadata["train_date_range"][1]
    if not isinstance(training_end, str):
        raise TypeError("Model metadata must declare a string training end")
    model = CatBoost()
    model.load_model(args.model_dir / "model.json", format="json")
    declared_names = metadata["feature_names"]
    if not isinstance(declared_names, list) or not all(
        isinstance(name, str) for name in declared_names
    ):
        raise TypeError("Model metadata must declare string predictor names")
    predictions = score_frozen_catboost(
        pl.read_parquet(args.features),
        model,
        training_end=training_end,
        feature_names=tuple(str(name) for name in declared_names),
        observed_only=not args.include_unlabelled,
        frame_loader=args.frame_loader,
    )
    args.output.mkdir(parents=True, exist_ok=True)
    predictions.write_parquet(args.output / "predictions.parquet")
    report = {
        "model_version": metadata["model_version"],
        "training_end": training_end,
        "races": predictions["race_id"].n_unique(),
        "rows": predictions.height,
        "first_date": predictions["race_date"].min(),
        "last_date": predictions["race_date"].max(),
        "metrics": None if args.include_unlabelled else exact_rank_metrics(predictions),
        "mode": "inference-only"
        if args.include_unlabelled
        else "observed-finisher-evaluation",
        "observed_labels": predictions.filter(pl.col("finish") > 0).height,
        "promotion_eligible": False,
        "projection": "predict_lib.scorer.build_feature_matrix:catboost",
        "frame_loader": args.frame_loader,
        "prophet_postprocessing_applied": False,
        "feature_order_contract": "named"
        if model.feature_names_ == declared_names
        else "positional-metadata",
        "caveat": "Feature/market parity not yet attested; inference mode never reports accuracy",
    }
    (args.output / "report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
