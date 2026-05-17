#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Aggregate running-style prediction JSONL into accuracy/macro-F1
metrics and UPSERT into running_style_model_evaluations.

Reads the JSONL emitted by running_style_lightgbm.py walk-forward
(rows carry target_running_style_class plus predicted_class). Rows
with NULL targets are excluded from precision/recall/accuracy but
still counted in prediction_count for full population reference.

Run with:
  .venv/bin/python src/scripts/finish-position-features/compare-running-style-predictions.py \\
    --jsonl tmp/finish-position-eval/predictions-jra/running-style-lgbm/2024-2025.jsonl \\
    --model-version jra-running-style-lgbm-v1.0 \\
    --category jra \\
    --window-from 20240101 --window-to 20251231
"""
from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from pathlib import Path
from typing import TypedDict

DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"
CLASS_NAMES: tuple[str, str, str, str] = ("nige", "senkou", "sashi", "oikomi")
NUM_CLASSES = 4
EVALUATIONS_TABLE = "running_style_model_evaluations"


class RunningStyleEvaluation(TypedDict):
    model_version: str
    category: str
    window_from: str
    window_to: str
    race_count: int
    prediction_count: int
    accuracy: float | None
    macro_f1: float | None
    precision_per_class: dict[str, float | None]
    recall_per_class: dict[str, float | None]
    support_per_class: dict[str, int]
    kyakushitsuhantei_agreement: float | None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="compare_running_style_predictions")
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


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and not math.isnan(value):
        return int(value)
    return None


def filter_labeled_records(records: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        record
        for record in records
        if _int_or_none(record.get("target_running_style_class")) is not None
        and _int_or_none(record.get("predicted_class")) is not None
    ]


def compute_accuracy(records: list[dict[str, object]]) -> float | None:
    if not records:
        return None
    correct = sum(
        1
        for record in records
        if _int_or_none(record.get("target_running_style_class"))
        == _int_or_none(record.get("predicted_class"))
    )
    return correct / len(records)


def _classify_counts(records: list[dict[str, object]]) -> tuple[list[int], list[int], list[int]]:
    true_positives = [0] * NUM_CLASSES
    predicted_counts = [0] * NUM_CLASSES
    actual_counts = [0] * NUM_CLASSES
    for record in records:
        actual = _int_or_none(record.get("target_running_style_class"))
        predicted = _int_or_none(record.get("predicted_class"))
        if actual is None or predicted is None:
            continue
        if 0 <= actual < NUM_CLASSES:
            actual_counts[actual] += 1
        if 0 <= predicted < NUM_CLASSES:
            predicted_counts[predicted] += 1
        if actual == predicted and 0 <= actual < NUM_CLASSES:
            true_positives[actual] += 1
    return true_positives, predicted_counts, actual_counts


def compute_per_class_metrics(
    records: list[dict[str, object]],
) -> tuple[dict[str, float | None], dict[str, float | None], dict[str, int]]:
    true_positives, predicted_counts, actual_counts = _classify_counts(records)
    precision: dict[str, float | None] = {}
    recall: dict[str, float | None] = {}
    support: dict[str, int] = {}
    for index, name in enumerate(CLASS_NAMES):
        precision[name] = (
            true_positives[index] / predicted_counts[index]
            if predicted_counts[index] > 0
            else None
        )
        recall[name] = (
            true_positives[index] / actual_counts[index]
            if actual_counts[index] > 0
            else None
        )
        support[name] = actual_counts[index]
    return precision, recall, support


def compute_macro_f1(
    precision: dict[str, float | None], recall: dict[str, float | None]
) -> float | None:
    f1_values: list[float] = []
    for name in CLASS_NAMES:
        p = precision[name]
        r = recall[name]
        if p is None or r is None or (p + r) == 0:
            continue
        f1_values.append(2.0 * p * r / (p + r))
    if not f1_values:
        return None
    return sum(f1_values) / len(f1_values)


def race_count(records: list[dict[str, object]]) -> int:
    race_ids: set[str] = set()
    for record in records:
        race_id = record.get("race_id")
        if isinstance(race_id, str):
            race_ids.add(race_id)
    return len(race_ids)


def build_evaluation(
    records: list[dict[str, object]],
    model_version: str,
    category: str,
    window_from: str,
    window_to: str,
) -> RunningStyleEvaluation:
    labeled = filter_labeled_records(records)
    precision, recall, support = compute_per_class_metrics(labeled)
    return {
        "accuracy": compute_accuracy(labeled),
        "category": category,
        "kyakushitsuhantei_agreement": None,
        "macro_f1": compute_macro_f1(precision, recall),
        "model_version": model_version,
        "precision_per_class": precision,
        "prediction_count": len(records),
        "race_count": race_count(records),
        "recall_per_class": recall,
        "support_per_class": support,
        "window_from": window_from,
        "window_to": window_to,
    }


def _format_numeric(value: float | None) -> str:
    if value is None:
        return "NULL"
    return f"{value:.6f}"


def build_upsert_sql(payload: RunningStyleEvaluation) -> str:
    precision = payload["precision_per_class"]
    recall = payload["recall_per_class"]
    support = payload["support_per_class"]
    return (
        f"insert into {EVALUATIONS_TABLE} ("
        "model_version, category, evaluation_window_from, evaluation_window_to, "
        "race_count, prediction_count, accuracy, macro_f1, "
        "precision_nige, precision_senkou, precision_sashi, precision_oikomi, "
        "recall_nige, recall_senkou, recall_sashi, recall_oikomi, "
        "support_nige, support_senkou, support_sashi, support_oikomi, "
        "kyakushitsuhantei_agreement) values ("
        f"'{payload['model_version']}', '{payload['category']}', "
        f"'{payload['window_from']}', '{payload['window_to']}', "
        f"{payload['race_count']}, {payload['prediction_count']}, "
        f"{_format_numeric(payload['accuracy'])}, "
        f"{_format_numeric(payload['macro_f1'])}, "
        f"{_format_numeric(precision['nige'])}, {_format_numeric(precision['senkou'])}, "
        f"{_format_numeric(precision['sashi'])}, {_format_numeric(precision['oikomi'])}, "
        f"{_format_numeric(recall['nige'])}, {_format_numeric(recall['senkou'])}, "
        f"{_format_numeric(recall['sashi'])}, {_format_numeric(recall['oikomi'])}, "
        f"{support['nige']}, {support['senkou']}, {support['sashi']}, {support['oikomi']}, "
        f"{_format_numeric(payload['kyakushitsuhantei_agreement'])}"
        ") on conflict (model_version, category, evaluation_window_from, evaluation_window_to) do update set "
        "race_count = excluded.race_count, "
        "prediction_count = excluded.prediction_count, "
        "accuracy = excluded.accuracy, macro_f1 = excluded.macro_f1, "
        "precision_nige = excluded.precision_nige, precision_senkou = excluded.precision_senkou, "
        "precision_sashi = excluded.precision_sashi, precision_oikomi = excluded.precision_oikomi, "
        "recall_nige = excluded.recall_nige, recall_senkou = excluded.recall_senkou, "
        "recall_sashi = excluded.recall_sashi, recall_oikomi = excluded.recall_oikomi, "
        "support_nige = excluded.support_nige, support_senkou = excluded.support_senkou, "
        "support_sashi = excluded.support_sashi, support_oikomi = excluded.support_oikomi, "
        "kyakushitsuhantei_agreement = excluded.kyakushitsuhantei_agreement, "
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
