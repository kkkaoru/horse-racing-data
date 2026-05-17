#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Grid-search (and optional scipy.optimize) blend weights for multiple model
JSONL predictions, maximizing NDCG@3 on a validation split.

For each base model, we rank-normalize within each race, then weighted-sum the
normalized ranks. Weights are searched on the validation split (e.g., 2024) and
the chosen weight is evaluated on a separate test split (e.g., 2025).

Run with:
  .venv/bin/python src/scripts/finish-position-features/optimize-ensemble-weights.py \
    --actuals tmp/finish-position-eval/actuals-2024-2025.csv \
    --jsonl name=lgbm:tmp/.../ensemble-v3.jsonl \
    --jsonl name=trans:tmp/.../transformer-ens3.jsonl \
    --validation-year 2024 --test-year 2025 \
    --output-best tmp/finish-position-eval/best-weights.json \
    --output-blend tmp/finish-position-eval/blended.jsonl
"""
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from itertools import product
from pathlib import Path

NDCG_K = 3
IDEAL_DCG_AT_3 = 3 / math.log(3) + 2 / math.log(4) + 1 / math.log(5)
WEIGHT_GRID_STEP = 0.05


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="optimize_ensemble_weights")
    parser.add_argument("--actuals", type=Path, required=True)
    parser.add_argument(
        "--jsonl",
        action="append",
        required=True,
        help="model JSONL: name=label:path",
    )
    parser.add_argument("--validation-year", type=int, required=True)
    parser.add_argument("--test-year", type=int, required=True)
    parser.add_argument("--output-best", type=Path, required=True)
    parser.add_argument("--output-blend", type=Path)
    parser.add_argument("--grid-step", type=float, default=WEIGHT_GRID_STEP)
    parser.add_argument(
        "--objective",
        choices=tuple(METRIC_FUNCTIONS.keys()),
        default="ndcg_at_3",
    )
    return parser.parse_args(argv)


def parse_model_spec(spec: str) -> tuple[str, Path]:
    if not spec.startswith("name="):
        raise ValueError(f"jsonl spec must be 'name=LABEL:PATH', got {spec!r}")
    name_value = spec[len("name="):]
    if ":" not in name_value:
        raise ValueError(f"jsonl spec missing ':' separator, got {spec!r}")
    label, path = name_value.split(":", 1)
    return label, Path(path)


def load_actuals(path: Path) -> dict[tuple[str, str], int]:
    actuals: dict[tuple[str, str], int] = {}
    with path.open("r", encoding="utf-8") as handle:
        header = handle.readline().rstrip("\n").split(",")
        rid_idx = header.index("race_id")
        kid_idx = header.index("ketto_toroku_bango")
        fp_idx = header.index("finish_position")
        for line in handle:
            cells = line.rstrip("\n").split(",")
            actuals[(cells[rid_idx], cells[kid_idx])] = int(cells[fp_idx])
    return actuals


def load_jsonl_scores(path: Path) -> dict[tuple[str, str], float]:
    scores: dict[tuple[str, str], float] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            row = json.loads(stripped)
            key = (str(row["race_id"]), str(row["ketto_toroku_bango"]))
            scores[key] = float(row["predicted_score"])
    return scores


def load_jsonl_umaban(path: Path) -> dict[tuple[str, str], int]:
    out: dict[tuple[str, str], int] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            row = json.loads(stripped)
            key = (str(row["race_id"]), str(row["ketto_toroku_bango"]))
            if "umaban" in row and row["umaban"] is not None:
                out[key] = int(row["umaban"])
    return out


def normalize_within_race(scores_by_race: dict[str, list[tuple[str, float]]]) -> dict[tuple[str, str], float]:
    out: dict[tuple[str, str], float] = {}
    for race_id, horse_scores in scores_by_race.items():
        n = len(horse_scores)
        if n <= 1:
            for horse_id, _ in horse_scores:
                out[(race_id, horse_id)] = 0.5
            continue
        sorted_by_score = sorted(horse_scores, key=lambda item: -item[1])
        for rank, (horse_id, _) in enumerate(sorted_by_score):
            out[(race_id, horse_id)] = (n - 1 - rank) / (n - 1)
    return out


def group_by_race(scores: dict[tuple[str, str], float]) -> dict[str, list[tuple[str, float]]]:
    grouped: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for (race_id, horse_id), score in scores.items():
        grouped[race_id].append((horse_id, score))
    return grouped


def race_id_year(race_id: str) -> int:
    return int(race_id.split(":")[1])


def filter_by_year(scores: dict[tuple[str, str], float], year: int) -> dict[tuple[str, str], float]:
    return {key: value for key, value in scores.items() if race_id_year(key[0]) == year}


def filter_actuals_by_year(actuals: dict[tuple[str, str], int], year: int) -> dict[tuple[str, str], int]:
    return {key: value for key, value in actuals.items() if race_id_year(key[0]) == year}


def _build_blended_groups(
    normalized_per_model: list[dict[tuple[str, str], float]],
    weights: list[float],
) -> dict[str, list[tuple[str, float]]]:
    blended: dict[tuple[str, str], float] = defaultdict(float)
    for normalized, w in zip(normalized_per_model, weights):
        for key, value in normalized.items():
            blended[key] += w * value
    grouped: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for (race_id, horse_id), score in blended.items():
        grouped[race_id].append((horse_id, score))
    return grouped


def compute_ndcg_at_3(
    normalized_per_model: list[dict[tuple[str, str], float]],
    weights: list[float],
    actuals: dict[tuple[str, str], int],
) -> float:
    grouped = _build_blended_groups(normalized_per_model, weights)
    total_ndcg = 0.0
    race_count = 0
    for race_id, horse_scores in grouped.items():
        sorted_by_score = sorted(horse_scores, key=lambda item: -item[1])
        dcg = 0.0
        for rank, (horse_id, _) in enumerate(sorted_by_score[:NDCG_K], start=1):
            finish_pos = actuals.get((race_id, horse_id))
            if finish_pos is None:
                continue
            gain = max(0, 4 - finish_pos)
            dcg += gain / math.log(2 + rank)
        total_ndcg += dcg / IDEAL_DCG_AT_3
        race_count += 1
    return total_ndcg / max(race_count, 1)


def compute_top3_exact(
    normalized_per_model: list[dict[tuple[str, str], float]],
    weights: list[float],
    actuals: dict[tuple[str, str], int],
) -> float:
    grouped = _build_blended_groups(normalized_per_model, weights)
    total_hit = 0
    race_count = 0
    for race_id, horse_scores in grouped.items():
        sorted_by_score = sorted(horse_scores, key=lambda item: -item[1])
        is_exact = (
            actuals.get((race_id, sorted_by_score[0][0])) == 1
            and len(sorted_by_score) >= 2
            and actuals.get((race_id, sorted_by_score[1][0])) == 2
            and len(sorted_by_score) >= 3
            and actuals.get((race_id, sorted_by_score[2][0])) == 3
        )
        total_hit += int(is_exact)
        race_count += 1
    return total_hit / max(race_count, 1)


def compute_top3_box(
    normalized_per_model: list[dict[tuple[str, str], float]],
    weights: list[float],
    actuals: dict[tuple[str, str], int],
) -> float:
    grouped = _build_blended_groups(normalized_per_model, weights)
    total_hit = 0
    race_count = 0
    for race_id, horse_scores in grouped.items():
        sorted_by_score = sorted(horse_scores, key=lambda item: -item[1])
        top3_ids = [horse_id for horse_id, _ in sorted_by_score[:3]]
        finish_positions = [actuals.get((race_id, hid)) for hid in top3_ids]
        is_box = all(fp is not None and 1 <= fp <= 3 for fp in finish_positions)
        total_hit += int(is_box)
        race_count += 1
    return total_hit / max(race_count, 1)


METRIC_FUNCTIONS = {
    "ndcg_at_3": compute_ndcg_at_3,
    "top3_exact": compute_top3_exact,
    "top3_box": compute_top3_box,
}


def enumerate_simplex_weights(num_models: int, step: float) -> list[list[float]]:
    steps = int(round(1.0 / step))
    candidates: list[list[float]] = []
    for combo in product(range(steps + 1), repeat=num_models - 1):
        last = steps - sum(combo)
        if last < 0:
            continue
        weights = [c * step for c in combo] + [last * step]
        candidates.append(weights)
    return candidates


def write_blended_jsonl(
    normalized_per_model: list[dict[tuple[str, str], float]],
    weights: list[float],
    scores_per_model: list[dict[tuple[str, str], float]],
    umaban_lookup: dict[tuple[str, str], int],
    output: Path,
) -> None:
    blended: dict[tuple[str, str], float] = defaultdict(float)
    for normalized, w in zip(normalized_per_model, weights):
        for key, value in normalized.items():
            blended[key] += w * value
    grouped: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for (race_id, horse_id), score in blended.items():
        grouped[race_id].append((horse_id, score))
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as handle:
        for race_id in sorted(grouped.keys()):
            ranked = sorted(grouped[race_id], key=lambda item: -item[1])
            for rank, (horse_id, score) in enumerate(ranked, start=1):
                row: dict[str, object] = {
                    "race_id": race_id,
                    "ketto_toroku_bango": horse_id,
                    "predicted_score": score,
                    "predicted_rank": rank,
                }
                umaban_value = umaban_lookup.get((race_id, horse_id))
                if umaban_value is not None:
                    row["umaban"] = umaban_value
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    args = parse_args()
    actuals = load_actuals(args.actuals)
    actuals_val = filter_actuals_by_year(actuals, int(args.validation_year))
    actuals_test = filter_actuals_by_year(actuals, int(args.test_year))
    model_specs = [parse_model_spec(s) for s in args.jsonl]
    names = [name for name, _ in model_specs]
    raw_scores = [load_jsonl_scores(path) for _, path in model_specs]
    val_normalized = [normalize_within_race(group_by_race(filter_by_year(s, int(args.validation_year)))) for s in raw_scores]
    test_normalized = [normalize_within_race(group_by_race(filter_by_year(s, int(args.test_year)))) for s in raw_scores]
    weight_candidates = enumerate_simplex_weights(len(names), float(args.grid_step))
    metric_fn = METRIC_FUNCTIONS[args.objective]
    best_val_score = -1.0
    best_weights = [1.0 / len(names)] * len(names)
    for weights in weight_candidates:
        score = metric_fn(val_normalized, weights, actuals_val)
        if score > best_val_score:
            best_val_score = score
            best_weights = weights
    test_score = metric_fn(test_normalized, best_weights, actuals_test)
    test_ndcg = compute_ndcg_at_3(test_normalized, best_weights, actuals_test)
    summary = {
        "models": names,
        "objective": args.objective,
        "best_weights": {name: float(w) for name, w in zip(names, best_weights)},
        "validation_year": int(args.validation_year),
        "validation_metric": best_val_score,
        "test_year": int(args.test_year),
        "test_metric": test_score,
        "test_ndcg_at_3": test_ndcg,
        "num_candidates_searched": len(weight_candidates),
    }
    args.output_best.parent.mkdir(parents=True, exist_ok=True)
    args.output_best.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.output_blend is not None:
        all_normalized = [normalize_within_race(group_by_race(s)) for s in raw_scores]
        umaban_lookup: dict[tuple[str, str], int] = {}
        for _, path in model_specs:
            umaban_lookup.update(load_jsonl_umaban(path))
        write_blended_jsonl(all_normalized, best_weights, raw_scores, umaban_lookup, args.output_blend)
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
