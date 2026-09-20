#!/usr/bin/env python3
"""Run portable MLP or LoMETab on the common nested walk-forward folds."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

import numpy as np

from timesfm_finish_position.lab_domain import PredictionFrame
from timesfm_finish_position.lab_metrics import evaluate_prediction_frame
from timesfm_finish_position.model_comparison import (
    CpuPerformance,
    benchmark_cpu_inference,
    evaluate_member_diversity,
)
from timesfm_finish_position.model_interface import ModelDataset, ModelPrediction, write_prediction
from timesfm_finish_position.neural_dataset import (
    DEFAULT_CATEGORICAL_NAMES,
    load_neural_dataset,
    subset_model_dataset,
)
from timesfm_finish_position.neural_models import (
    NeuralModelConfig,
    NeuralModelKind,
    PortableNeuralModel,
)
from timesfm_finish_position.neural_preprocessing import (
    NeuralFeatureConfig,
    NumericEncoding,
)
from timesfm_finish_position.portable_inference import (
    evaluate_backend_parity,
    predict_portable_mlp,
)
from timesfm_finish_position.tabular_evaluation import (
    fit_temperature,
    nested_masks,
    scores_to_probabilities,
)

DEFAULT_INPUT = Path("tmp/nar-tabular-pit-current-state-core-v1.parquet")
DEFAULT_HISTORY = Path("tmp/nar-horse-history-current-state-2020-2026.parquet")
DEFAULT_OUTPUT = Path("tmp/neural-current-state-v1")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--model", choices=[kind.value for kind in NeuralModelKind], required=True)
    parser.add_argument(
        "--numeric-encoding",
        choices=[encoding.value for encoding in NumericEncoding],
        default=NumericEncoding.NORMALIZED.value,
    )
    parser.add_argument("--categorical", action="append", dest="categorical_names")
    parser.add_argument("--numeric-only", action="store_true")
    parser.add_argument("--year", type=int, action="append", dest="years")
    parser.add_argument("--epochs", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument("--hidden-dimension", type=int, action="append", dest="hidden_dimensions")
    parser.add_argument("--dropout", type=float, default=0.1)
    parser.add_argument("--learning-rate", type=float, default=0.002)
    parser.add_argument("--ensemble-size", type=int, default=8)
    parser.add_argument("--adapter-rank", type=int, default=4)
    parser.add_argument("--initialization-scale", type=float, default=0.3)
    parser.add_argument("--device", choices=("auto", "cpu", "mps", "cuda"), default="cpu")
    parser.add_argument("--accelerated-parity-backend", choices=("none", "mlx"), default="none")
    return parser.parse_args()


def _prediction_frame(
    data: ModelDataset,
    prediction: ModelPrediction,
) -> PredictionFrame:
    if data.finish_positions is None:
        raise RuntimeError("evaluation partition has no labels")
    return PredictionFrame(
        race_ids=data.race_ids,
        race_dates=data.race_dates,
        horse_ids=data.horse_ids,
        finish_positions=data.finish_positions,
        decimal_odds=data.decimal_odds,
        win_probabilities=prediction.probability,
        ranking_scores=prediction.prediction,
    )


def _benchmark_model(
    model: PortableNeuralModel,
    data: ModelDataset,
    artifact_path: Path,
) -> CpuPerformance:
    return benchmark_cpu_inference(
        infer=lambda: model.predict(data),
        cold_load=lambda: PortableNeuralModel.load(artifact_path),
        artifact_path=artifact_path,
    )


def run(args: argparse.Namespace) -> list[dict[str, object]]:
    years = tuple(args.years or (2024, 2025, 2026))
    categorical_names = tuple(args.categorical_names or ())
    if args.numeric_only and categorical_names:
        raise ValueError("--numeric-only and --categorical are mutually exclusive")
    if (
        args.model != NeuralModelKind.PLAIN_MLP.value
        and not args.numeric_only
        and not categorical_names
    ):
        categorical_names = DEFAULT_CATEGORICAL_NAMES
    dataset = load_neural_dataset(
        args.input,
        args.history,
        categorical_names=categorical_names,
    )
    args.output.mkdir(parents=True, exist_ok=True)
    reports: list[dict[str, object]] = []
    for year in years:
        train_mask, calibration_mask, test_mask = nested_masks(dataset.race_dates, year)
        model_version = f"{args.model}-current-state-v1-{year}"
        config = NeuralModelConfig(
            kind=NeuralModelKind(args.model),
            model_version=model_version,
            hidden_dimensions=tuple(args.hidden_dimensions or (128, 64)),
            dropout=args.dropout,
            learning_rate=args.learning_rate,
            epochs=args.epochs,
            batch_size=args.batch_size,
            ensemble_size=args.ensemble_size,
            adapter_rank=args.adapter_rank,
            initialization_scale=args.initialization_scale,
            training_device=args.device,
        )
        feature_config = NeuralFeatureConfig(
            numeric_encoding=NumericEncoding(args.numeric_encoding),
            categorical_names=categorical_names,
        )
        model = PortableNeuralModel(
            config,
            feature_config,
            feature_version="nar-current-state-core-v1",
        )
        model.fit(subset_model_dataset(dataset, train_mask))
        calibration = subset_model_dataset(dataset, calibration_mask)
        calibration_prediction = model.predict(calibration)
        if calibration.finish_positions is None:
            raise RuntimeError("calibration partition has no labels")
        temperature = fit_temperature(
            calibration_prediction.prediction,
            calibration.race_ids,
            calibration.finish_positions,
        )
        test = subset_model_dataset(dataset, test_mask)
        raw_prediction = model.predict(test)
        probability = scores_to_probabilities(
            raw_prediction.prediction,
            test.race_ids,
            temperature=temperature,
        )
        prediction = ModelPrediction(
            race_ids=test.race_ids,
            horse_ids=test.horse_ids,
            prediction=raw_prediction.prediction,
            probability=probability,
            model_name=raw_prediction.model_name,
            model_version=model_version,
        )
        artifact_path = args.output / "artifacts" / str(year)
        model.save(artifact_path)
        write_prediction(args.output / "predictions" / f"{model_version}.parquet", prediction)
        restored = PortableNeuralModel.load(artifact_path)
        restored_prediction = restored.predict(test)
        portable_max_abs_error = float(
            np.max(np.abs(raw_prediction.prediction - restored_prediction.prediction))
        )
        performance = _benchmark_model(restored, test, artifact_path)
        diversity = None
        if args.model == NeuralModelKind.LOMETAB.value:
            diversity = asdict(
                evaluate_member_diversity(test.race_ids, restored.predict_members(test))
            )
        accelerated_parity = None
        if (
            args.accelerated_parity_backend != "none"
            and args.model != NeuralModelKind.LOMETAB.value
        ):
            portable_reference = predict_portable_mlp(artifact_path, test, backend="numpy")
            portable_candidate = predict_portable_mlp(
                artifact_path, test, backend=args.accelerated_parity_backend
            )
            accelerated_parity = asdict(
                evaluate_backend_parity(portable_reference, portable_candidate, test.race_ids)
            )
        report = {
            "year": year,
            "model": args.model,
            "model_version": model_version,
            "train_rows": int(np.sum(train_mask)),
            "calibration_rows": int(np.sum(calibration_mask)),
            "test_rows": int(np.sum(test_mask)),
            "temperature": temperature,
            "numeric_encoding": args.numeric_encoding,
            "categorical_names": categorical_names,
            "metrics": asdict(evaluate_prediction_frame(_prediction_frame(test, prediction))),
            "member_diversity": diversity,
            "portable_cpu": asdict(performance),
            "portable_max_abs_error": portable_max_abs_error,
            "accelerated_parity": accelerated_parity,
            "production_integration": False,
        }
        reports.append(report)
        (args.output / f"{model_version}.json").write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(json.dumps(report, sort_keys=True), flush=True)
    return reports


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
