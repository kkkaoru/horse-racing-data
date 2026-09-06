#!/usr/bin/env python3
"""Train official TabM with nested walk-forward calibration on local PIT data."""

from __future__ import annotations

import argparse
import json
import platform
from dataclasses import asdict
from pathlib import Path

import numpy as np
import torch
from tabm import TabM
from torch.utils.data import DataLoader, TensorDataset

from timesfm_finish_position.lab_metrics import evaluate_prediction_frame
from timesfm_finish_position.tabular_evaluation import (
    TabularDataset,
    TreeFoldResult,
    _prediction_frame,
    apply_imputation,
    fit_imputation,
    fit_temperature,
    load_tabular_dataset,
    nested_masks,
    write_prediction_frame,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("tmp/nar-tabular-pit-2020-2026.parquet"))
    parser.add_argument("--output", type=Path, default=Path("tmp/tabm-lab"))
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--epochs", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=4096)
    return parser.parse_args()


def resolve_device() -> str:
    if platform.system() == "Darwin" and platform.machine() == "arm64":
        return "mps"
    if torch.cuda.is_available():
        return "cuda"
    return "cpu"


def normalized_features(
    dataset: TabularDataset, train: np.ndarray, selected: np.ndarray
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    medians = fit_imputation(dataset.features[train])
    train_values = apply_imputation(dataset.features[train], medians)
    means = np.mean(train_values, axis=0)
    scales = np.std(train_values, axis=0)
    scales[scales < 1e-8] = 1.0
    values = apply_imputation(dataset.features[selected], medians)
    return ((values - means) / scales).astype(np.float32), means, scales


def predict(model: TabM, values: np.ndarray, device: str, batch_size: int) -> np.ndarray:
    model.eval()
    outputs: list[np.ndarray] = []
    with torch.inference_mode():
        for start in range(0, len(values), batch_size):
            tensor = torch.from_numpy(values[start : start + batch_size]).to(device)
            logits = model(tensor).squeeze(-1).mean(dim=1)
            outputs.append(logits.detach().cpu().numpy().astype(np.float64))
    return np.concatenate(outputs)


def run(args: argparse.Namespace) -> TreeFoldResult:
    torch.manual_seed(20260902)
    dataset = load_tabular_dataset(args.input)
    train, calibration, test = nested_masks(dataset.race_dates, args.year)
    train_values, means, scales = normalized_features(dataset, train, train)
    calibration_values = (
        apply_imputation(dataset.features[calibration], fit_imputation(dataset.features[train]))
        - means
    ) / scales
    test_values = (
        apply_imputation(dataset.features[test], fit_imputation(dataset.features[train])) - means
    ) / scales
    labels = (dataset.finish_positions[train] == 1).astype(np.float32)
    device = resolve_device()
    model = TabM.make(n_num_features=train_values.shape[1], d_out=1).to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=0.002, weight_decay=0.0003)
    positive_weight = torch.tensor([(len(labels) - labels.sum()) / labels.sum()], device=device)
    loss_function = torch.nn.BCEWithLogitsLoss(pos_weight=positive_weight)
    generator = torch.Generator().manual_seed(20260902)
    loader = DataLoader(
        TensorDataset(torch.from_numpy(train_values), torch.from_numpy(labels)),
        batch_size=args.batch_size,
        shuffle=True,
        generator=generator,
    )
    model.train()
    for epoch in range(args.epochs):
        total_loss = 0.0
        for features, target in loader:
            optimizer.zero_grad(set_to_none=True)
            logits = model(features.to(device)).squeeze(-1)
            loss = loss_function(logits, target.to(device)[:, None].expand_as(logits))
            loss.backward()
            optimizer.step()
            total_loss += float(loss.detach().cpu())
        print(json.dumps({"epoch": epoch + 1, "loss": total_loss / len(loader)}), flush=True)
    calibration_scores = predict(
        model, calibration_values.astype(np.float32), device, args.batch_size
    )
    temperature = fit_temperature(
        calibration_scores, dataset.race_ids[calibration], dataset.finish_positions[calibration]
    )
    test_scores = predict(model, test_values.astype(np.float32), device, args.batch_size)
    frame = _prediction_frame(dataset, test, test_scores, temperature)
    result = TreeFoldResult(
        model="tabm",
        year=args.year,
        train_rows=int(np.sum(train)),
        calibration_rows=int(np.sum(calibration)),
        test_rows=int(np.sum(test)),
        temperature=temperature,
        metrics=evaluate_prediction_frame(frame),
    )
    args.output.mkdir(parents=True, exist_ok=True)
    write_prediction_frame(args.output / f"tabm-{args.year}.parquet", frame)
    (args.output / f"tabm-{args.year}.json").write_text(
        json.dumps({"device": device, **asdict(result)}, indent=2) + "\n"
    )
    return result


def main() -> None:
    print(json.dumps(asdict(run(parse_args())), sort_keys=True))


if __name__ == "__main__":
    main()
