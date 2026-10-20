#!/usr/bin/env python3
"""Train odds-free PyTorch MPS race-set models for Nakayama and Hanshin cells."""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Final

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import torch

from timesfm_finish_position.pytorch_mpx import (
    RaceSetTransformer,
    graded_relevance,
    listnet_loss,
    pad_races,
    resolve_mpx_device,
    select_numeric_features,
    winner_contained_hits,
)

REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[3]
CONTAINER_SRC: Final[Path] = REPO_ROOT / "apps/finish-position-predict-container/src"
if str(CONTAINER_SRC) not in sys.path:
    sys.path.insert(0, str(CONTAINER_SRC))

DEFAULT_FEATURES: Final[Path] = Path(
    "/Users/kkk4oru/.local/share/horse-racing-data-production/jra-cell-v1/features-2000-2026"
)
DEFAULT_BASELINE: Final[Path] = Path(
    "/Users/kkk4oru/.local/share/horse-racing-data-production/"
    "jra-cell-v1/current-v61-exact-cell-accuracy-2024-2026.json"
)
VENUES: Final[dict[str, str]] = {"06": "nakayama", "09": "hanshin"}
EVAL_YEARS: Final[tuple[int, ...]] = (2024, 2025, 2026)
IDENTITY_LOAD: Final[tuple[str, ...]] = (
    "race_id",
    "race_date",
    "race_year",
    "keibajo_code",
    "ketto_toroku_bango",
    "umaban",
    "finish_position",
    "kyori",
    "track_code",
    "kyoso_joken_code",
    "grade_code",
    "kyosomei_hondai",
)
BATCH_RACES = 64
EMPTY_HITS: Final[dict[str, int]] = {
    "top1": 0,
    "top2": 0,
    "top3": 0,
    "top4": 0,
    "top5": 0,
}


def _empty_hits() -> dict[str, int]:
    return dict(EMPTY_HITS)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="train_pytorch_mpx_venue_cells")
    parser.add_argument("--features-root", type=Path, default=DEFAULT_FEATURES)
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--epochs", type=int, default=8)
    parser.add_argument("--embedding-dim", type=int, default=64)
    parser.add_argument("--heads", type=int, default=4)
    parser.add_argument("--layers", type=int, default=2)
    parser.add_argument("--learning-rate", type=float, default=1e-3)
    parser.add_argument("--weight-decay", type=float, default=1e-5)
    parser.add_argument("--dropout", type=float, default=0.0)
    parser.add_argument("--device", default="auto")
    parser.add_argument("--seed", type=int, default=20260921)
    parser.add_argument("--venues", default="06,09")
    parser.add_argument(
        "--train-venues",
        default="",
        help="Venues used for training. Empty means the same as --venues.",
    )
    parser.add_argument("--finetune-epochs", type=int, default=0)
    parser.add_argument("--batch-races", type=int, default=BATCH_RACES)
    parser.add_argument("--distance", type=int, default=0)
    parser.add_argument("--surface", default="")
    parser.add_argument("--class-code", default="")
    parser.add_argument("--checkpoint-dir", type=Path)
    parser.add_argument("--trends-dir", type=Path)
    return parser.parse_args(argv)


def _codes(raw: str) -> list[str]:
    return [code.zfill(2) for code in raw.split(",") if code.strip()]


def policy_race_id(race_id: str) -> str:
    parts = str(race_id).split(":")
    if len(parts) != 5:
        return str(race_id)
    return f"{parts[0]}:{parts[1]}{parts[2]}:{parts[3]}:{parts[4]}"


def _year(frame: pd.DataFrame) -> pd.Series:
    return frame["race_date"].astype(str).str[:4].astype(int)


def load_frame(features_root: Path, venues: list[str]) -> tuple[pd.DataFrame, list[str]]:
    dataset = ds.dataset(features_root, format="parquet", partitioning="hive")
    available = list(dataset.schema.names)
    missing = [name for name in IDENTITY_LOAD if name not in available]
    if missing:
        raise ValueError(f"feature snapshot missing required columns: {missing}")
    numeric = select_numeric_features(available)
    present_numeric = [name for name in numeric if name in available]
    columns = list(dict.fromkeys([*IDENTITY_LOAD, *present_numeric]))
    table = dataset.to_table(columns=columns)
    frame = table.to_pandas()
    if frame.columns.duplicated().any():
        frame = frame.loc[:, ~frame.columns.duplicated()].copy()
    frame["keibajo_code"] = frame["keibajo_code"].astype(str).str.zfill(2)
    frame = frame[frame["keibajo_code"].isin(venues)].copy()
    frame["race_id"] = frame["race_id"].astype(str)
    frame["race_date"] = frame["race_date"].astype(str)
    frame["ketto_toroku_bango"] = frame["ketto_toroku_bango"].astype(str)
    frame["finish_position"] = pd.to_numeric(frame["finish_position"], errors="coerce")
    frame = frame[frame["finish_position"].notna() & (frame["finish_position"] >= 1)].reset_index(
        drop=True
    )
    return frame, present_numeric


def _numeric_frame(frame: pd.DataFrame, names: list[str]) -> pd.DataFrame:
    return pd.DataFrame({name: pd.to_numeric(frame[name], errors="coerce") for name in names})


def _normalise(frame: pd.DataFrame, names: list[str]) -> tuple[np.ndarray, np.ndarray]:
    numeric = _numeric_frame(frame, names)
    mean = numeric.mean().fillna(0.0).to_numpy(dtype=np.float64)
    scale = numeric.std(ddof=0).fillna(0.0).to_numpy(dtype=np.float64)
    return mean, np.where(scale > 1e-12, scale, 1.0)


def _matrix(
    frame: pd.DataFrame, names: list[str], mean: np.ndarray, scale: np.ndarray
) -> np.ndarray:
    numeric = _numeric_frame(frame, names).fillna(0.0)
    values = numeric.to_numpy(dtype=np.float64)
    if values.shape[1] != len(names):
        raise ValueError("numeric matrix width must match the feature list")
    return ((values - mean) / scale).astype(np.float32)


def _race_groups(frame: pd.DataFrame) -> list[np.ndarray]:
    grouped = frame.groupby("race_id", sort=False).indices.values()
    return [np.asarray(rows, dtype=np.int64) for rows in grouped]


def _samples(
    frame: pd.DataFrame, names: list[str], mean: np.ndarray, scale: np.ndarray
) -> tuple[list[np.ndarray], list[np.ndarray], list[np.ndarray], list[np.ndarray]]:
    values = _matrix(frame, names, mean, scale)
    positions = frame["finish_position"].to_numpy(dtype=np.float64)
    umaban = pd.to_numeric(frame["umaban"], errors="coerce").to_numpy(dtype=np.float64)
    features: list[np.ndarray] = []
    targets: list[np.ndarray] = []
    labels: list[np.ndarray] = []
    groups: list[np.ndarray] = []
    for rows in _race_groups(frame):
        order = np.argsort(umaban[rows], kind="stable")
        ordered = rows[order]
        race_positions = positions[ordered]
        relevance = graded_relevance(race_positions)
        if float(relevance.sum()) <= 0.0:
            continue
        features.append(values[ordered])
        targets.append(relevance)
        labels.append(race_positions)
        groups.append(ordered)
    return features, targets, labels, groups


def train_model(
    features: list[np.ndarray],
    targets: list[np.ndarray],
    *,
    width: int,
    args: argparse.Namespace,
    device: torch.device,
    model: RaceSetTransformer | None = None,
    epochs: int | None = None,
) -> RaceSetTransformer:
    # Seed per call: venue fine-tunes otherwise inherit the dropout RNG stream left by the
    # previous venue, so each venue's result depended on venue iteration order.
    torch.manual_seed(args.seed)
    if model is None:
        model = RaceSetTransformer(
            width,
            embedding_dim=args.embedding_dim,
            heads=args.heads,
            layers=args.layers,
            dropout=args.dropout,
        ).to(device)
    run_epochs = args.epochs if epochs is None else epochs
    optimizer = torch.optim.AdamW(
        model.parameters(), lr=args.learning_rate, weight_decay=args.weight_decay
    )
    generator = torch.Generator().manual_seed(args.seed)
    for epoch in range(run_epochs):
        model.train()
        total = 0.0
        batches = 0
        order = torch.randperm(len(features), generator=generator).tolist()
        batch_races = args.batch_races
        for start in range(0, len(order), batch_races):
            chunk = order[start : start + batch_races]
            padded, relevance, mask = pad_races(
                [features[index] for index in chunk],
                [targets[index] for index in chunk],
                width=width,
            )
            logits = model(padded.to(device), mask.to(device))
            loss = listnet_loss(logits, relevance.to(device), mask.to(device))
            if not torch.isfinite(loss):
                raise ValueError(f"Nonfinite training loss at epoch {epoch + 1}")
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            total += float(loss.detach())
            batches += 1
        print(
            f"    epoch {epoch + 1}/{run_epochs} loss={total / max(batches, 1):.5f}",
            flush=True,
        )
    return model


def score_races(
    model: RaceSetTransformer,
    features: list[np.ndarray],
    width: int,
    device: torch.device,
) -> list[np.ndarray]:
    model.eval()
    scores: list[np.ndarray] = []
    with torch.no_grad():
        for start in range(0, len(features), BATCH_RACES):
            chunk = features[start : start + BATCH_RACES]
            dummy = [graded_relevance(np.arange(1, matrix.shape[0] + 1)) for matrix in chunk]
            padded, _targets, mask = pad_races(chunk, dummy, width=width)
            logits = model(padded.to(device), mask.to(device)).cpu().numpy()
            for row, matrix in enumerate(chunk):
                scores.append(logits[row, : matrix.shape[0]])
    return scores


def dump_cell_trends(
    args: argparse.Namespace,
    frame: pd.DataFrame,
    names: list[str],
    device: torch.device,
) -> int:
    if args.checkpoint_dir is None or args.trends_dir is None:
        raise ValueError("checkpoint-dir and trends-dir are required to dump trends")
    years = _year(frame)
    args.trends_dir.mkdir(parents=True, exist_ok=True)
    for eval_year in EVAL_YEARS:
        stem = args.checkpoint_dir / f"jra-{eval_year}"
        payload = json.loads(stem.with_suffix(".json").read_text(encoding="utf-8"))
        mean = np.asarray(payload["mean"], dtype=np.float64)
        scale = np.asarray(payload["scale"], dtype=np.float64)
        model = RaceSetTransformer(
            len(names),
            embedding_dim=args.embedding_dim,
            heads=args.heads,
            layers=args.layers,
            dropout=args.dropout,
        ).to(device)
        state = torch.load(stem.with_suffix(".pt"), map_location=device, weights_only=True)
        model.load_state_dict(state)
        eval_frame = frame.loc[years == eval_year].reset_index(drop=True)
        features, _targets, _positions, groups = _samples(eval_frame, names, mean, scale)
        scores = score_races(model, features, len(names), device)
        race_ids: list[str] = []
        horses: list[str] = []
        ranking: list[float] = []
        for score, rows in zip(scores, groups, strict=True):
            race = policy_race_id(str(eval_frame.iloc[int(rows[0])]["race_id"]))
            ids = eval_frame.iloc[rows]["ketto_toroku_bango"].astype(str).tolist()
            race_ids.extend(race for _horse in ids)
            horses.extend(ids)
            ranking.extend(float(value) for value in score)
        path = args.trends_dir / f"prophet-entity-trends-{eval_year}.parquet"
        pq.write_table(
            pa.table(
                {
                    "race_id": race_ids,
                    "horse_id": horses,
                    "ranking_score": pa.array(ranking, type=pa.float64()),
                }
            ),
            path,
        )
        print(f"wrote {path} rows={len(ranking)}", flush=True)
    return 0


def _parse_race_date(value: str) -> date:
    text = value.replace("-", "")[:8]
    return date(int(text[:4]), int(text[4:6]), int(text[6:8]))


def cell_key(frame: pd.DataFrame, rows: np.ndarray) -> str:
    from predict_lib.jra_cell_scope import JraRace, cell_for_race

    first = frame.iloc[int(rows[0])]
    raw_distance = pd.to_numeric(pd.Series([first["kyori"]]), errors="coerce").iloc[0]
    distance = 1 if pd.isna(raw_distance) or int(raw_distance) <= 0 else int(raw_distance)
    race = JraRace(
        race_id=str(first["race_id"]),
        race_date=_parse_race_date(str(first["race_date"])),
        venue=str(first["keibajo_code"]).zfill(2),
        distance=distance,
        track_code=str(first["track_code"]),
        condition_code=str(first["kyoso_joken_code"]).zfill(3),
        grade_code=str(first["grade_code"]),
        race_name="" if pd.isna(first["kyosomei_hondai"]) else str(first["kyosomei_hondai"]),
        race_type_code="",
        weight_type_code="",
        horse_ids=tuple(frame.iloc[rows]["ketto_toroku_bango"].astype(str)),
    )
    return cell_for_race(race).canonical


def _hits_dict(hits: dict[str, int], races: int) -> dict[str, float | int]:
    return {
        **hits,
        "race_count": races,
        **{f"{key}_rate": (hits[key] / races if races else 0.0) for key in hits},
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    venues = _codes(args.venues)
    train_venues = _codes(args.train_venues) if args.train_venues else venues
    device = resolve_mpx_device(args.device)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    print(f"device={device} eval={venues} train={train_venues}", flush=True)
    started = time.time()
    frame, names = load_frame(args.features_root, sorted(set(train_venues) | set(venues)))
    if args.distance > 0:
        frame = frame[pd.to_numeric(frame["kyori"], errors="coerce") == args.distance]
    if args.surface:
        from predict_lib.jra_cell_scope import derive_surface

        surfaces = frame["track_code"].astype(str).map(derive_surface)
        frame = frame[surfaces == args.surface]
    if args.class_code:
        wanted = args.class_code.zfill(3)
        codes = frame["kyoso_joken_code"].astype(str).str.zfill(3)
        frame = frame[codes == wanted]
    frame = frame.reset_index(drop=True)
    years = _year(frame)
    print(
        f"rows={len(frame)} features={len(names)} load_s={time.time() - started:.1f} "
        f"distance={args.distance or '-'} surface={args.surface or '-'} "
        f"class={args.class_code or '-'}",
        flush=True,
    )
    if args.checkpoint_dir is not None:
        return dump_cell_trends(args, frame, names, device)
    banned = [name for name in frame.columns if "odds" in name.lower() or "tansho" in name.lower()]
    if any(name in names for name in banned):
        raise RuntimeError(f"odds features leaked into training columns: {banned}")

    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    production = {
        str(cell["canonical"]): cell["metrics"]["current_hits"] for cell in baseline["cells"]
    }
    production_races = {
        str(cell["canonical"]): int(cell["metrics"]["race_count"]) for cell in baseline["cells"]
    }

    stats: dict[str, dict[str, object]] = {
        venue: {
            "hits": _empty_hits(),
            "races": 0,
            "cell_hits": defaultdict(_empty_hits),
            "cell_races": defaultdict(int),
        }
        for venue in venues
    }
    for eval_year in EVAL_YEARS:
        train_mask = frame["keibajo_code"].isin(train_venues) & (years < eval_year)
        train_frame = frame.loc[train_mask].reset_index(drop=True)
        if train_frame.empty:
            print(f"  skip {eval_year}: empty train", flush=True)
            continue
        mean, scale = _normalise(train_frame, names)
        train_x, train_y, _train_pos, _train_rows = _samples(train_frame, names, mean, scale)
        print(f"  year {eval_year} train_races={len(train_x)}", flush=True)
        model = train_model(train_x, train_y, width=len(names), args=args, device=device)
        pretrained = {
            key: value.detach().cpu().clone() for key, value in model.state_dict().items()
        }
        torch.save(model.state_dict(), args.output_dir / f"jra-{eval_year}.pt")
        (args.output_dir / f"jra-{eval_year}.json").write_text(
            json.dumps(
                {
                    "eval_year": eval_year,
                    "train_venues": train_venues,
                    "feature_order": names,
                    "mean": mean.tolist(),
                    "scale": scale.tolist(),
                }
            )
            + "\n",
            encoding="utf-8",
        )
        for venue in venues:
            eval_mask = (frame["keibajo_code"] == venue) & (years == eval_year)
            eval_frame = frame.loc[eval_mask].reset_index(drop=True)
            if eval_frame.empty:
                print(f"    skip {VENUES.get(venue, venue)} {eval_year}: empty eval", flush=True)
                continue
            eval_x, _eval_y, eval_pos, eval_rows = _samples(eval_frame, names, mean, scale)
            scorer = model
            if args.finetune_epochs > 0:
                scorer = RaceSetTransformer(
                    len(names),
                    embedding_dim=args.embedding_dim,
                    heads=args.heads,
                    layers=args.layers,
                    dropout=args.dropout,
                ).to(device)
                scorer.load_state_dict(pretrained)
                venue_train = frame.loc[
                    (frame["keibajo_code"] == venue) & (years < eval_year)
                ].reset_index(drop=True)
                fine_x, fine_y, _fine_pos, _fine_rows = _samples(venue_train, names, mean, scale)
                print(
                    f"    finetune {VENUES.get(venue, venue)} races={len(fine_x)}",
                    flush=True,
                )
                scorer = train_model(
                    fine_x,
                    fine_y,
                    width=len(names),
                    args=args,
                    device=device,
                    model=scorer,
                    epochs=args.finetune_epochs,
                )
            scores = score_races(scorer, eval_x, len(names), device)
            year_hits = winner_contained_hits(scores, eval_pos)
            bucket = stats[venue]
            hits = bucket["hits"]
            cell_hits = bucket["cell_hits"]
            cell_races = bucket["cell_races"]
            if not isinstance(hits, dict) or not isinstance(cell_hits, dict):
                raise TypeError("venue stats must keep hit maps")
            if not isinstance(cell_races, dict):
                raise TypeError("venue stats must keep race counts")
            for key, value in year_hits.items():
                hits[key] += value
            races = bucket["races"]
            if not isinstance(races, int):
                raise TypeError("venue stats must keep race counts")
            bucket["races"] = races + len(scores)
            print(
                f"    {VENUES.get(venue, venue)} {eval_year} eval={len(scores)} "
                + " ".join(f"{key}={value / len(scores):.4%}" for key, value in year_hits.items()),
                flush=True,
            )
            for score, rows in zip(scores, eval_rows, strict=True):
                canonical = cell_key(eval_frame, rows)
                cell_races[canonical] += 1
                finish = eval_frame.iloc[rows]["finish_position"].to_numpy()
                race_hits = winner_contained_hits([score], [finish])
                for key, value in race_hits.items():
                    cell_hits[canonical][key] += value

    venue_reports: dict[str, object] = {}
    for venue in venues:
        bucket = stats[venue]
        hits = bucket["hits"]
        cell_hits = bucket["cell_hits"]
        cell_races = bucket["cell_races"]
        overall_races = bucket["races"]
        if not isinstance(overall_races, int):
            raise TypeError("venue stats must keep race counts")
        if not isinstance(hits, dict) or not isinstance(cell_hits, dict):
            raise TypeError("venue stats must keep hit maps")
        if not isinstance(cell_races, dict):
            raise TypeError("venue stats must keep race counts")
        improved: list[dict[str, object]] = []
        lagged: list[dict[str, object]] = []
        for canonical, cell in sorted(cell_hits.items()):
            current = production.get(str(canonical))
            if current is None or not isinstance(cell, dict):
                continue
            delta = {key: int(cell[key]) - int(current[key]) for key in cell}
            record = {
                "canonical": canonical,
                "race_count": cell_races[canonical],
                "production_race_count": production_races.get(str(canonical)),
                "model_hits": cell,
                "production_hits": current,
                "delta_hits": delta,
            }
            vector = [delta["top1"], delta["top2"], delta["top3"], delta["top4"], delta["top5"]]
            if vector > [0, 0, 0, 0, 0]:
                improved.append(record)
            elif vector < [0, 0, 0, 0, 0]:
                lagged.append(record)
        venue_reports[VENUES.get(venue, venue)] = {
            "venue": venue,
            "overall": _hits_dict(hits, overall_races),
            "improved_cells": improved,
            "lagged_cells": lagged,
            "cell_count": len(cell_hits),
        }
        overall = _hits_dict(hits, overall_races)
        print(
            f"  {VENUES.get(venue, venue)} overall races={overall_races} "
            + " ".join(f"{key}={overall[f'{key}_rate']:.4%}" for key in hits)
            + f" improved_cells={len(improved)}",
            flush=True,
        )

    report = {
        "version": "pytorch-mpx-race-set-nakayama-hanshin-v1",
        "device": str(device),
        "features": names,
        "feature_count": len(names),
        "odds_banned": True,
        "venues": venue_reports,
    }
    output = args.output_dir / "evaluation-2024-2026.json"
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
