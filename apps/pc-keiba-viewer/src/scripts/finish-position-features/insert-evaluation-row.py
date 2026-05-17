#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Insert or update one row in `model_prediction_evaluations` from a metrics JSON
(compare-model-metrics.py output). Updates Local Postgres only — Neon sync is
handled separately by `apps/local-postgresql/scripts/push-neon-sync.ts`.

Run with:
  .venv/bin/python src/scripts/finish-position-features/insert-evaluation-row.py \
    --metrics-json tmp/finish-position-eval/metrics-v3-final.json \
    --metrics-label v3 \
    --model-version jra-trans-lgbm-ensemble-v3 \
    --category jra \
    --window-from 20240101 --window-to 20251231 \
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
METRIC_NUMERIC_COLUMNS: tuple[str, ...] = (
    "top1_accuracy",
    "place1_accuracy",
    "place2_accuracy",
    "place3_accuracy",
    "top3_box_accuracy",
    "top3_exact_accuracy",
    "top3_winner_capture",
    "pair_score",
    "ndcg_at_3",
    "top3_place_relation",
)


class EvaluationPayload(TypedDict):
    model_version: str
    category: str
    evaluation_window_from: str
    evaluation_window_to: str
    race_count: int
    prediction_count: int
    metric_values: dict[str, float | None]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="insert_evaluation_row")
    parser.add_argument("--metrics-json", type=Path, required=True)
    parser.add_argument(
        "--metrics-label",
        type=str,
        required=True,
        help="label inside metrics-json's 'results' array to source from",
    )
    parser.add_argument("--model-version", type=str, required=True)
    parser.add_argument("--category", type=str, default="jra")
    parser.add_argument("--window-from", type=str, required=True)
    parser.add_argument("--window-to", type=str, required=True)
    parser.add_argument(
        "--pg-url",
        type=str,
        default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL),
    )
    return parser.parse_args(argv)


def select_row_by_label(metrics_payload: dict[str, object], label: str) -> dict[str, object]:
    results = metrics_payload.get("results")
    if not isinstance(results, list):
        raise ValueError("metrics JSON missing 'results' array")
    for raw_row in results:
        if not isinstance(raw_row, dict):
            continue
        row: dict[str, object] = {str(k): v for k, v in raw_row.items()}
        if row.get("label") == label:
            return row
    raise KeyError(f"label {label!r} not found in metrics results")


def _as_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        return int(value)
    return 0


def build_payload(
    metrics_row: dict[str, object],
    model_version: str,
    category: str,
    window_from: str,
    window_to: str,
) -> EvaluationPayload:
    metric_values: dict[str, float | None] = {}
    for column in METRIC_NUMERIC_COLUMNS:
        raw = metrics_row.get(column)
        metric_values[column] = float(raw) if isinstance(raw, (int, float)) else None
    return {
        "model_version": model_version,
        "category": category,
        "evaluation_window_from": window_from,
        "evaluation_window_to": window_to,
        "race_count": _as_int(metrics_row.get("race_count")),
        "prediction_count": _as_int(metrics_row.get("prediction_count")),
        "metric_values": metric_values,
    }


def format_value(value: float | None) -> str:
    if value is None:
        return "NULL"
    return f"{value:.6f}"


def build_sql(payload: EvaluationPayload) -> str:
    cols = (
        "model_version, category, evaluation_window_from, evaluation_window_to,"
        " race_count, prediction_count, "
        + ", ".join(METRIC_NUMERIC_COLUMNS)
    )
    values = [
        f"'{payload['model_version']}'",
        f"'{payload['category']}'",
        f"'{payload['evaluation_window_from']}'",
        f"'{payload['evaluation_window_to']}'",
        str(payload["race_count"]),
        str(payload["prediction_count"]),
        *[format_value(payload["metric_values"][column]) for column in METRIC_NUMERIC_COLUMNS],
    ]
    excluded = ",\n  ".join(
        [
            "race_count = excluded.race_count",
            "prediction_count = excluded.prediction_count",
            *[f"{column} = excluded.{column}" for column in METRIC_NUMERIC_COLUMNS],
            "evaluated_at = now()",
        ]
    )
    return (
        f"insert into model_prediction_evaluations ({cols})\n"
        f"values ({', '.join(values)})\n"
        f"on conflict (model_version, category, evaluation_window_from, evaluation_window_to)\n"
        f"do update set\n  {excluded};"
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
    print(result.stdout.strip())


def main() -> None:
    args = parse_args()
    metrics_payload = json.loads(args.metrics_json.read_text(encoding="utf-8"))
    metrics_row = select_row_by_label(metrics_payload, args.metrics_label)
    payload = build_payload(
        metrics_row,
        args.model_version,
        args.category,
        args.window_from,
        args.window_to,
    )
    sql = build_sql(payload)
    run_psql(args.pg_url, sql)
    print(
        json.dumps(
            {
                "model_version": payload["model_version"],
                "evaluation_window": [payload["evaluation_window_from"], payload["evaluation_window_to"]],
                "race_count": payload["race_count"],
                "metrics": payload["metric_values"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
