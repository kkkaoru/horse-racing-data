#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Aggregate corner-position prediction JSONL into per-corner MAE and
top-3 agreement, then UPSERT into corner_position_model_evaluations.

Reads the JSONL emitted by corner_position_lightgbm.py walk-forward
(rows carry both target_corner_n_norm and corner_n_pred). Skip rows
where the actual target is NULL.

Run with:
  .venv/bin/python src/scripts/finish-position-features/compare-corner-position-predictions.py \\
    --jsonl tmp/finish-position-eval/predictions-jra/corner-lgbm/2024-2025.jsonl \\
    --model-version jra-corner-position-lgbm-v1.0 \\
    --category jra \\
    --window-from 20240101 --window-to 20251231 \\
    --pg-url postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import TypedDict

DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"
CORNER_HEADS: tuple[str, str, str] = ("corner_1", "corner_3", "corner_4")
TOP_K_AGREEMENT = 3
EVALUATIONS_TABLE = "corner_position_model_evaluations"


class CornerEvaluation(TypedDict):
    model_version: str
    category: str
    window_from: str
    window_to: str
    race_count: int
    prediction_count: int
    corner_1_mae: float | None
    corner_3_mae: float | None
    corner_4_mae: float | None
    mean_mae: float | None
    corner_1_top3_agreement: float | None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="compare_corner_position_predictions")
    parser.add_argument("--jsonl", type=Path, required=True)
    parser.add_argument("--model-version", type=str, required=True)
    parser.add_argument("--category", type=str, default="jra")
    parser.add_argument("--window-from", type=str, required=True)
    parser.add_argument("--window-to", type=str, required=True)
    parser.add_argument(
        "--pg-url",
        type=str,
        default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL),
    )
    parser.add_argument(
        "--skip-pg-insert",
        action="store_true",
        help="Compute metrics without writing to Postgres",
    )
    return parser.parse_args(argv)


def load_jsonl(path: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if line == "":
                continue
            records.append(json.loads(line))
    return records


def _numeric_or_none(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, bool):
            return None
        return float(value)
    return None


def mae_for_head(records: list[dict[str, object]], head: str) -> float | None:
    target_column = f"target_{head}_norm"
    pred_column = f"{head}_pred"
    total = 0.0
    count = 0
    for record in records:
        target = _numeric_or_none(record.get(target_column))
        predicted = _numeric_or_none(record.get(pred_column))
        if target is None or predicted is None:
            continue
        total += abs(target - predicted)
        count += 1
    if count == 0:
        return None
    return total / count


def mean_mae(per_head: dict[str, float | None]) -> float | None:
    values: list[float] = [value for value in per_head.values() if value is not None]
    if not values:
        return None
    return sum(values) / len(values)


def _group_by_race(records: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    groups: dict[str, list[dict[str, object]]] = {}
    for record in records:
        race_id = record.get("race_id")
        if not isinstance(race_id, str):
            continue
        groups.setdefault(race_id, []).append(record)
    return groups


def _rank_ascending(values: list[float]) -> list[int]:
    indexed = sorted(range(len(values)), key=lambda idx: values[idx])
    ranks = [0] * len(values)
    for rank_offset, original_index in enumerate(indexed):
        ranks[original_index] = rank_offset + 1
    return ranks


def corner_1_top3_agreement(records: list[dict[str, object]]) -> float | None:
    groups = _group_by_race(records)
    totals = 0.0
    races = 0
    for race_runners in groups.values():
        valid_runners = [
            row for row in race_runners
            if _numeric_or_none(row.get("target_corner_1_norm")) is not None
            and _numeric_or_none(row.get("corner_1_pred")) is not None
        ]
        if len(valid_runners) < TOP_K_AGREEMENT:
            continue
        actuals = [float(row["target_corner_1_norm"]) for row in valid_runners]
        predicts = [float(row["corner_1_pred"]) for row in valid_runners]
        actual_ranks = _rank_ascending(actuals)
        predicted_ranks = _rank_ascending(predicts)
        actual_top3 = {idx for idx, rank in enumerate(actual_ranks) if rank <= TOP_K_AGREEMENT}
        predicted_top3 = {idx for idx, rank in enumerate(predicted_ranks) if rank <= TOP_K_AGREEMENT}
        intersection_size = len(actual_top3 & predicted_top3)
        totals += intersection_size / TOP_K_AGREEMENT
        races += 1
    if races == 0:
        return None
    return totals / races


def race_count(records: list[dict[str, object]]) -> int:
    return len(_group_by_race(records))


def build_evaluation(
    records: list[dict[str, object]],
    model_version: str,
    category: str,
    window_from: str,
    window_to: str,
) -> CornerEvaluation:
    per_head: dict[str, float | None] = {head: mae_for_head(records, head) for head in CORNER_HEADS}
    return {
        "category": category,
        "corner_1_mae": per_head["corner_1"],
        "corner_3_mae": per_head["corner_3"],
        "corner_4_mae": per_head["corner_4"],
        "corner_1_top3_agreement": corner_1_top3_agreement(records),
        "mean_mae": mean_mae(per_head),
        "model_version": model_version,
        "prediction_count": len(records),
        "race_count": race_count(records),
        "window_from": window_from,
        "window_to": window_to,
    }


def _format_numeric(value: float | None) -> str:
    if value is None:
        return "NULL"
    return f"{value:.6f}"


def build_upsert_sql(payload: CornerEvaluation) -> str:
    return (
        f"insert into {EVALUATIONS_TABLE} ("
        "model_version, category, evaluation_window_from, evaluation_window_to, "
        "race_count, prediction_count, "
        "corner_1_mae, corner_3_mae, corner_4_mae, mean_mae, corner_1_top3_agreement) values ("
        f"'{payload['model_version']}', '{payload['category']}', "
        f"'{payload['window_from']}', '{payload['window_to']}', "
        f"{payload['race_count']}, {payload['prediction_count']}, "
        f"{_format_numeric(payload['corner_1_mae'])}, "
        f"{_format_numeric(payload['corner_3_mae'])}, "
        f"{_format_numeric(payload['corner_4_mae'])}, "
        f"{_format_numeric(payload['mean_mae'])}, "
        f"{_format_numeric(payload['corner_1_top3_agreement'])}"
        ") on conflict (model_version, category, evaluation_window_from, evaluation_window_to) do update set "
        "race_count = excluded.race_count, "
        "prediction_count = excluded.prediction_count, "
        "corner_1_mae = excluded.corner_1_mae, "
        "corner_3_mae = excluded.corner_3_mae, "
        "corner_4_mae = excluded.corner_4_mae, "
        "mean_mae = excluded.mean_mae, "
        "corner_1_top3_agreement = excluded.corner_1_top3_agreement, "
        "evaluated_at = now();"
    )


def run_psql(pg_url: str, sql: str) -> None:
    result = subprocess.run(
        ["psql", pg_url, "-v", "ON_ERROR_STOP=1", "-c", sql],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"psql failed: {result.stderr.strip()}")


def main() -> None:
    args = parse_args()
    records = load_jsonl(args.jsonl)
    evaluation = build_evaluation(
        records, args.model_version, args.category, args.window_from, args.window_to,
    )
    if not args.skip_pg_insert:
        run_psql(args.pg_url, build_upsert_sql(evaluation))
    print(json.dumps(evaluation, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
