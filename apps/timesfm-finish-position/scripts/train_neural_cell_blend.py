#!/usr/bin/env python3
"""Train the portable neural cell-blend ranker on the production feature set.

The ranker is a small PyTorch MLP trained on MPS with a race-listwise graded
relevance loss over the same feature rows the Container already builds for its
tree models. Outputs are (a) per-runner trend parquets for the cell-policy
evaluator and (b) the portable JSON artifact the Container serves.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import torch
from torch import nn

REQUIRED_COLUMNS: tuple[str, ...] = (
    "race_id",
    "race_date",
    "ketto_toroku_bango",
    "finish_position",
)
REPO_ROOT = Path(__file__).resolve().parents[3]
ARTIFACT_VERSION = "neural-cell-blend-v1"
DROPOUT = 0.1
BATCH_RACES = 256
SCORE_BATCH = 256


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="train_neural_cell_blend")
    parser.add_argument("--category", choices=("jra", "nar", "ban-ei"), required=True)
    parser.add_argument("--features-root", type=Path, required=True)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--train-from", type=int, default=2000)
    parser.add_argument("--test-year", type=int, default=2026)
    parser.add_argument("--epochs", type=int, default=30)
    parser.add_argument("--hidden", type=int, default=256)
    parser.add_argument("--learning-rate", type=float, default=1e-3)
    parser.add_argument("--weight-decay", type=float, default=1e-5)
    parser.add_argument("--device", default="auto")
    parser.add_argument("--seed", type=int, default=20260920)
    return parser.parse_args(argv)


def resolve_device(preference: str) -> torch.device:
    """Return the requested accelerator, falling back to CPU when unavailable."""
    if preference == "auto":
        return torch.device("mps" if torch.backends.mps.is_available() else "cpu")
    if preference == "mps" and not torch.backends.mps.is_available():
        return torch.device("cpu")
    return torch.device(preference)


def load_metadata_features(path: Path) -> list[str]:
    document: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError("model metadata must be an object")
    names = document.get("feature_names")
    if not isinstance(names, list) or not names:
        raise ValueError("model metadata feature_names must be a non-empty list")
    return [str(name) for name in names]


def load_frame(
    features_root: Path, names: list[str], year_from: int, year_to: int
) -> tuple[pd.DataFrame, list[str]]:
    dataset = ds.dataset(features_root, format="parquet", partitioning="hive")
    available = set(dataset.schema.names)
    selected = [name for name in names if name in available]
    if not selected:
        raise ValueError("no metadata feature is present in the feature snapshot")
    table = dataset.to_table(
        columns=[*REQUIRED_COLUMNS, *selected],
        filter=(ds.field("race_year") >= year_from) & (ds.field("race_year") <= year_to),
    )
    frame = table.to_pandas()
    frame["race_id"] = frame["race_id"].astype(str)
    frame["race_date"] = frame["race_date"].astype(str)
    frame["ketto_toroku_bango"] = frame["ketto_toroku_bango"].astype(str)
    frame = frame[pd.to_numeric(frame["finish_position"], errors="coerce").notna()]
    return frame.reset_index(drop=True), selected


def normalisation(frame: pd.DataFrame, names: list[str]) -> tuple[np.ndarray, np.ndarray]:
    numeric = frame.loc[:, names].apply(pd.to_numeric, errors="coerce")
    mean = numeric.mean().fillna(0.0).to_numpy(dtype=np.float64)
    scale = numeric.std(ddof=0).fillna(0.0).to_numpy(dtype=np.float64)
    scale = np.where(scale > 1e-12, scale, 1.0)
    return mean, scale


def build_samples(
    frame: pd.DataFrame,
    names: list[str],
    mean: np.ndarray,
    scale: np.ndarray,
) -> list[tuple[np.ndarray, np.ndarray]]:
    numeric = frame.loc[:, names].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    values = ((numeric.to_numpy(dtype=np.float64) - mean) / scale).astype(np.float32)
    positions = pd.to_numeric(frame["finish_position"], errors="coerce").to_numpy(dtype=np.float64)
    samples: list[tuple[np.ndarray, np.ndarray]] = []
    for rows in frame.groupby("race_id", sort=False).indices.values():
        race_positions = positions[rows]
        relevance = np.zeros(len(rows), dtype=np.float64)
        for index, position in enumerate(race_positions):
            if math.isfinite(position) and position >= 1:
                relevance[index] = 1.0 / math.log2(1.0 + float(position))
        if relevance.sum() <= 0:
            continue
        samples.append((values[rows], relevance / relevance.sum()))
    return samples


def train_model(
    samples: list[tuple[np.ndarray, np.ndarray]],
    *,
    width: int,
    args: argparse.Namespace,
    device: torch.device,
) -> nn.Sequential:
    model = nn.Sequential(
        nn.Linear(width, args.hidden),
        nn.GELU(),
        nn.Dropout(DROPOUT),
        nn.Linear(args.hidden, args.hidden // 2),
        nn.GELU(),
        nn.Linear(args.hidden // 2, 1),
    ).to(device)
    optimizer = torch.optim.AdamW(
        model.parameters(), lr=args.learning_rate, weight_decay=args.weight_decay
    )
    generator = torch.Generator().manual_seed(args.seed)
    order = torch.randperm(len(samples), generator=generator).tolist()
    for epoch in range(args.epochs):
        model.train()
        total = 0.0
        batches = 0
        for start in range(0, len(order), BATCH_RACES):
            chunk = [samples[index] for index in order[start : start + BATCH_RACES]]
            features, targets, mask = _batch(chunk, width)
            logits = model(features.to(device)).squeeze(-1).masked_fill(~mask.to(device), -1e9)
            loss = (
                -(targets.to(device) * nn.functional.log_softmax(logits, dim=-1)).sum(dim=-1).mean()
            )
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            total += float(loss.detach())
            batches += 1
        print(f"  epoch {epoch + 1}/{args.epochs} loss={total / max(batches, 1):.5f}", flush=True)
    return model


def _batch(
    chunk: list[tuple[np.ndarray, np.ndarray]], width: int
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    longest = max(len(values) for values, _target in chunk)
    features = torch.zeros((len(chunk), longest, width))
    targets = torch.zeros((len(chunk), longest))
    mask = torch.zeros((len(chunk), longest), dtype=torch.bool)
    for row, (values, target) in enumerate(chunk):
        features[row, : len(values)] = torch.tensor(values)
        targets[row, : len(target)] = torch.tensor(target)
        mask[row, : len(values)] = True
    return features, targets, mask


def score_races(
    model: nn.Sequential,
    samples: list[tuple[np.ndarray, np.ndarray]],
    width: int,
    device: torch.device,
) -> list[np.ndarray]:
    model.eval()
    results: list[np.ndarray] = []
    with torch.no_grad():
        for start in range(0, len(samples), SCORE_BATCH):
            chunk = samples[start : start + SCORE_BATCH]
            features, _targets, mask = _batch(chunk, width)
            logits = model(features.to(device)).squeeze(-1).masked_fill(~mask.to(device), -1e9)
            for row, (values, _target) in enumerate(chunk):
                results.append(logits[row, : len(values)].cpu().numpy())
    return results


def winner_topk(
    scores: list[np.ndarray],
    samples: list[tuple[np.ndarray, np.ndarray]],
    positions: list[np.ndarray],
) -> dict[str, float]:
    hits = {rank: 0 for rank in range(1, 6)}
    for score, _sample, race_positions in zip(scores, samples, positions, strict=True):
        order = np.argsort(-score, kind="stable")
        winner = int(np.argmin(race_positions))
        rank = int(np.where(order == winner)[0][0]) + 1
        for cutoff in range(1, 6):
            if rank <= cutoff:
                hits[cutoff] += 1
    return {f"top{rank}": hits[rank] / len(scores) for rank in range(1, 6)}


def artifact_document(
    category: str,
    names: list[str],
    mean: np.ndarray,
    scale: np.ndarray,
    model: nn.Sequential,
) -> dict[str, object]:
    layers: list[dict[str, object]] = []
    for module in model:
        if isinstance(module, nn.Linear):
            layers.append(
                {
                    "weight": [
                        [float(value) for value in row]
                        for row in module.weight.detach().cpu().tolist()
                    ],
                    "bias": [float(value) for value in module.bias.detach().cpu().tolist()],
                }
            )
    return {
        "version": ARTIFACT_VERSION,
        "category": category,
        "feature_order": names,
        "mean": [float(value) for value in mean],
        "scale": [float(value) for value in scale],
        "layers": layers,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    device = resolve_device(args.device)
    names = load_metadata_features(args.metadata)
    print(f"category={args.category} device={device} metadata_features={len(names)}", flush=True)
    started = time.time()
    frame, selected = load_frame(args.features_root, names, args.train_from, args.test_year)
    print(
        f"  rows={len(frame)} selected_features={len(selected)} in {time.time() - started:.1f}s",
        flush=True,
    )
    years = frame["race_date"].str[:4]
    train_mask = years < str(args.test_year - 1)
    mean, scale = normalisation(frame[train_mask], selected)
    train_frame = frame[train_mask]
    train_samples = build_samples(train_frame, selected, mean, scale)
    test_frame = frame[years == str(args.test_year)].reset_index(drop=True)
    test_samples = build_samples(test_frame, selected, mean, scale)
    print(f"  train_races={len(train_samples)} test_races={len(test_samples)}", flush=True)
    model = train_model(train_samples, width=len(selected), args=args, device=device)
    test_positions = [
        pd.to_numeric(test_frame.iloc[rows]["finish_position"], errors="coerce").to_numpy()
        for rows in test_frame.groupby("race_id", sort=False).indices.values()
    ]
    scores = score_races(model, test_samples, len(selected), device)
    print(
        "  test "
        + " ".join(
            f"{key}={value:.4%}"
            for key, value in winner_topk(scores, test_samples, test_positions).items()
        ),
        flush=True,
    )

    race_ids = list(test_frame.groupby("race_id", sort=False).indices.keys())
    horses = [
        test_frame.iloc[rows]["ketto_toroku_bango"].astype(str).tolist()
        for rows in test_frame.groupby("race_id", sort=False).indices.values()
    ]
    args.output_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "race_id": [
                    race for race, group in zip(race_ids, horses, strict=True) for _ in group
                ],
                "horse_id": [horse for group in horses for horse in group],
                "ranking_score": [float(value) for group in scores for value in group],
            }
        ),
        args.output_dir / f"neural-trends-{args.test_year}.parquet",
    )
    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    document = artifact_document(args.category, selected, mean, scale, model)
    args.artifact.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    _assert_serving_parity(document, test_frame, test_samples, scores)
    print(f"  wrote {args.output_dir} and {args.artifact}", flush=True)
    return 0


def _assert_serving_parity(
    document: dict[str, object],
    frame: pd.DataFrame,
    samples: list[tuple[np.ndarray, np.ndarray]],
    scores: list[np.ndarray],
) -> None:
    """Fail closed when the Container forward pass diverges from PyTorch."""
    sys.path.insert(0, str(REPO_ROOT / "apps/finish-position-predict-container/src"))
    from predict_lib.neural_blend import parse_neural_blend_artifact

    artifact = parse_neural_blend_artifact(document)
    names = list(artifact.feature_order)
    raw = frame.loc[:, names].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    offset = 0
    maximum = 0.0
    for rows, race_scores in zip(
        frame.groupby("race_id", sort=False).indices.values(), scores, strict=True
    ):
        if len(rows) != len(race_scores):
            continue
        entries = [
            {
                name: float(value)
                for name, value in zip(names, raw.iloc[rows[index]].to_numpy(), strict=True)
            }
            for index in range(len(rows))
        ]
        portable = artifact.score(entries)
        maximum = max(
            maximum,
            max(abs(left - right) for left, right in zip(portable, race_scores, strict=True)),
        )
        offset += len(rows)
        if offset >= 2000:
            break
    if maximum > 1e-3:
        raise RuntimeError(f"portable serving forward diverges from PyTorch: {maximum}")
    print(f"  serving parity max|diff|={maximum:.2e}", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
