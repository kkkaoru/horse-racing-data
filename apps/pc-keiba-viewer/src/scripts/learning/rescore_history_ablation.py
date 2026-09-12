"""Re-score frozen research models after local-PG data freshness corrections."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl
from catboost import CatBoostRanker

from learning.history_ablation import BASE_FEATURES, SPEED_FEATURES, predict_ablation
from learning.relative_history_features import (
    RELATIVE_SPEED_FEATURES,
    add_relative_history_features,
)


@dataclass(frozen=True)
class RescoreConfig:
    venue: str
    start: date
    end: date


def rescore_saved_ablation(
    feature_path: Path,
    model_dir: Path,
    output_dir: Path,
    *,
    config: RescoreConfig,
) -> dict[str, object]:
    metadata = json.loads((model_dir / "report.json").read_text(encoding="utf-8"))
    training_end = date.fromisoformat(metadata["training_end"])
    if training_end >= config.start or config.start > config.end:
        raise ValueError(
            "Re-scoring must be strictly after training and chronologically ordered"
        )
    venue = metadata.get("venue", metadata.get("dimensions", {}).get("venue"))
    if venue != config.venue:
        raise ValueError("Re-scoring venue differs from the trained model")
    model = CatBoostRanker()
    model.load_model(model_dir / "model.cbm")
    features = tuple(model.feature_names_)
    allowed = set(BASE_FEATURES + SPEED_FEATURES + RELATIVE_SPEED_FEATURES)
    if (
        not features
        or set(features).difference(allowed)
        or list(features) != metadata["features"]
    ):
        raise ValueError("Saved research feature contract is invalid")
    predicate = (pl.col("venue") == config.venue) & pl.col("race_date").is_between(
        pl.lit(config.start.strftime("%Y%m%d")), pl.lit(config.end.strftime("%Y%m%d"))
    )
    if set(features).intersection(RELATIVE_SPEED_FEATURES):
        frame = add_relative_history_features(pl.read_parquet(feature_path)).filter(
            predicate
        )
    else:
        frame = pl.scan_parquet(feature_path).filter(predicate).collect()
    result = predict_ablation(model, frame, features=features)
    output_dir.mkdir(parents=True, exist_ok=True)
    result.predictions.write_parquet(output_dir / "predictions.parquet")
    report: dict[str, object] = {
        "model_dir": str(model_dir),
        "input_features": str(feature_path),
        "training_end": training_end.isoformat(),
        "venue": config.venue,
        "races": result.predictions["race_id"].n_unique(),
        "rows": result.predictions.height,
        "metrics": result.metrics,
        "features": features,
        "refitted": False,
        "projection": "research-float32-preserve-NaN",
        "promotion_eligible": False,
    }
    (output_dir / "report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    return report


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--venue", required=True)
    parser.add_argument("--from-date", type=date.fromisoformat, required=True)
    parser.add_argument("--to-date", type=date.fromisoformat, required=True)
    args = parser.parse_args(argv)
    report = rescore_saved_ablation(
        args.features,
        args.model_dir,
        args.output,
        config=RescoreConfig(venue=args.venue, start=args.from_date, end=args.to_date),
    )
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
