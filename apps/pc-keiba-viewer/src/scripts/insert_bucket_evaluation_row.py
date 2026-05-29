"""Insert or update one row in `model_prediction_bucket_evaluations` from a metrics JSON.

Run with:
  uv run python src/scripts/insert_bucket_evaluation_row.py \
    --metrics-json tmp/bucket-eval/row.json \
    --model-version ensemble-v3 \
    --running-style-feature-version v1 \
    --finish-position-version v1 \
    --category jra \
    --window-from 20240101 --window-to 20251231 \
    --pg-url postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing
"""

from __future__ import annotations

import argparse
import importlib
import json
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Callable, Protocol, TypedDict

BUCKET_TABLE = "model_prediction_bucket_evaluations"

METRIC_SUM_COLUMNS: tuple[str, ...] = (
    "top1_hit_sum",
    "place1_hit_sum",
    "place2_hit_sum",
    "place3_hit_sum",
    "top3_box_hit_sum",
    "top3_exact_hit_sum",
    "top3_winner_capture_sum",
    "top5_winner_capture_sum",
    "top3_place_relation_sum",
    "pair_score_sum",
    "ndcg_at_3_sum",
)

INT_METRIC_COLUMNS: tuple[str, ...] = (
    "race_count",
    "prediction_count",
    "pair_score_pair_count",
    "ndcg_at_3_race_count",
)

DIMENSION_COLUMNS: tuple[str, ...] = (
    "model_version",
    "running_style_feature_version",
    "finish_position_version",
    "category",
    "evaluation_window_from",
    "evaluation_window_to",
    "source",
    "keibajo_code",
    "kyori",
    "kyoso_shubetsu_code",
    "kyoso_joken_code",
    "condition_key",
    "track_code",
    "grade_code",
    "race_name",
)

CONFLICT_COLUMNS: tuple[str, ...] = (
    "model_version",
    "running_style_feature_version",
    "finish_position_version",
    "category",
    "evaluation_window_from",
    "evaluation_window_to",
    "source",
    "keibajo_code",
    "kyori",
    "kyoso_shubetsu_code",
    "coalesce(kyoso_joken_code,'')",
    "coalesce(condition_key,'')",
    "coalesce(track_code,'')",
    "coalesce(grade_code,'')",
    "coalesce(race_name,'')",
)


class BucketRowPayload(TypedDict):
    source: str
    keibajo_code: str
    kyori: int
    kyoso_shubetsu_code: str
    kyoso_joken_code: str | None
    condition_key: str | None
    track_code: str | None
    grade_code: str | None
    race_name: str | None
    race_count: int
    prediction_count: int
    pair_score_pair_count: int
    ndcg_at_3_race_count: int
    top1_hit_sum: float
    place1_hit_sum: float
    place2_hit_sum: float
    place3_hit_sum: float
    top3_box_hit_sum: float
    top3_exact_hit_sum: float
    top3_winner_capture_sum: float
    top5_winner_capture_sum: float
    top3_place_relation_sum: float
    pair_score_sum: float
    ndcg_at_3_sum: float


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="insert_bucket_evaluation_row")
    parser.add_argument("--pg-url", type=str, required=True)
    parser.add_argument("--metrics-json", type=Path, required=True)
    parser.add_argument("--model-version", type=str, required=True)
    parser.add_argument("--running-style-feature-version", type=str, required=True)
    parser.add_argument("--finish-position-version", type=str, required=True)
    parser.add_argument("--category", type=str, required=True)
    parser.add_argument("--window-from", type=str, required=True)
    parser.add_argument("--window-to", type=str, required=True)
    return parser.parse_args(argv)


def to_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value:
        return int(float(value))
    return 0


def to_float(value: object) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value:
        return float(value)
    return 0.0


def to_optional_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def normalize_row(raw_row: dict[str, object]) -> BucketRowPayload:
    return BucketRowPayload(
        source=str(raw_row.get("source", "")),
        keibajo_code=str(raw_row.get("keibajo_code", "")),
        kyori=to_int(raw_row.get("kyori")),
        kyoso_shubetsu_code=str(raw_row.get("kyoso_shubetsu_code", "")),
        kyoso_joken_code=to_optional_str(raw_row.get("kyoso_joken_code")),
        condition_key=to_optional_str(raw_row.get("condition_key")),
        track_code=to_optional_str(raw_row.get("track_code")),
        grade_code=to_optional_str(raw_row.get("grade_code")),
        race_name=to_optional_str(raw_row.get("race_name")),
        race_count=to_int(raw_row.get("race_count")),
        prediction_count=to_int(raw_row.get("prediction_count")),
        pair_score_pair_count=to_int(raw_row.get("pair_score_pair_count")),
        ndcg_at_3_race_count=to_int(raw_row.get("ndcg_at_3_race_count")),
        top1_hit_sum=to_float(raw_row.get("top1_hit_sum")),
        place1_hit_sum=to_float(raw_row.get("place1_hit_sum")),
        place2_hit_sum=to_float(raw_row.get("place2_hit_sum")),
        place3_hit_sum=to_float(raw_row.get("place3_hit_sum")),
        top3_box_hit_sum=to_float(raw_row.get("top3_box_hit_sum")),
        top3_exact_hit_sum=to_float(raw_row.get("top3_exact_hit_sum")),
        top3_winner_capture_sum=to_float(raw_row.get("top3_winner_capture_sum")),
        top5_winner_capture_sum=to_float(raw_row.get("top5_winner_capture_sum")),
        top3_place_relation_sum=to_float(raw_row.get("top3_place_relation_sum")),
        pair_score_sum=to_float(raw_row.get("pair_score_sum")),
        ndcg_at_3_sum=to_float(raw_row.get("ndcg_at_3_sum")),
    )


def parse_metrics_payload(raw_text: str) -> list[dict[str, object]]:
    parsed = json.loads(raw_text)
    if isinstance(parsed, list):
        return [row for row in parsed if isinstance(row, dict)]
    if isinstance(parsed, dict):
        rows_field = parsed.get("rows")
        if isinstance(rows_field, list):
            return [row for row in rows_field if isinstance(row, dict)]
        raise ValueError("metrics JSON object must contain 'rows' array")
    raise ValueError("metrics JSON must be either an array or an object with 'rows'")


def build_insert_columns() -> list[str]:
    return list(DIMENSION_COLUMNS) + list(INT_METRIC_COLUMNS) + list(METRIC_SUM_COLUMNS)


def build_upsert_sql() -> str:
    columns = build_insert_columns()
    set_fragments = ",\n  ".join(
        [f"{col} = excluded.{col}" for col in columns if col not in DIMENSION_COLUMNS]
        + ["evaluated_at = now()"]
    )
    conflict_keys = ", ".join(CONFLICT_COLUMNS)
    column_list = ", ".join(columns)
    return (
        f"INSERT INTO {BUCKET_TABLE} ({column_list}, evaluated_at)\n"
        f"VALUES %s\n"
        f"ON CONFLICT ({conflict_keys}) DO UPDATE SET\n  {set_fragments}"
    )


def build_row_template() -> str:
    columns = build_insert_columns()
    placeholders = ", ".join(["%s"] * len(columns))
    return f"({placeholders}, now())"


def build_row_tuple(
    row: BucketRowPayload,
    model_version: str,
    running_style_feature_version: str,
    finish_position_version: str,
    category: str,
    window_from: str,
    window_to: str,
) -> tuple[object, ...]:
    return (
        model_version,
        running_style_feature_version,
        finish_position_version,
        category,
        window_from,
        window_to,
        row["source"],
        row["keibajo_code"],
        row["kyori"],
        row["kyoso_shubetsu_code"],
        row["kyoso_joken_code"],
        row["condition_key"],
        row["track_code"],
        row["grade_code"],
        row["race_name"],
        row["race_count"],
        row["prediction_count"],
        row["pair_score_pair_count"],
        row["ndcg_at_3_race_count"],
        row["top1_hit_sum"],
        row["place1_hit_sum"],
        row["place2_hit_sum"],
        row["place3_hit_sum"],
        row["top3_box_hit_sum"],
        row["top3_exact_hit_sum"],
        row["top3_winner_capture_sum"],
        row["top5_winner_capture_sum"],
        row["top3_place_relation_sum"],
        row["pair_score_sum"],
        row["ndcg_at_3_sum"],
    )


class PgCursor(Protocol):
    pass


class PgConnection(Protocol):
    def cursor(self) -> AbstractContextManager[PgCursor]: ...
    def commit(self) -> None: ...


def _default_psycopg_connect(pg_url: str) -> AbstractContextManager[PgConnection]:
    module = importlib.import_module("psycopg")
    return module.connect(pg_url)


def _default_execute_values() -> Callable[..., None]:
    module = importlib.import_module("psycopg.extras")
    return module.execute_values


def execute_upsert(
    pg_url: str,
    rows: list[tuple[object, ...]],
    connect: Callable[[str], AbstractContextManager[PgConnection]] = _default_psycopg_connect,
    execute_values_fn: Callable[..., None] | None = None,
) -> None:
    sql = build_upsert_sql()
    template = build_row_template()
    runner = execute_values_fn if execute_values_fn is not None else _default_execute_values()
    with connect(pg_url) as connection:
        with connection.cursor() as cursor:
            runner(cursor, sql, rows, template=template)
        connection.commit()


def main() -> None:
    args = parse_args()
    raw_text = args.metrics_json.read_text(encoding="utf-8")
    payload_rows = parse_metrics_payload(raw_text)
    normalized = [normalize_row(row) for row in payload_rows]
    tuples = [
        build_row_tuple(
            row,
            args.model_version,
            args.running_style_feature_version,
            args.finish_position_version,
            args.category,
            args.window_from,
            args.window_to,
        )
        for row in normalized
    ]
    execute_upsert(args.pg_url, tuples)
    print(
        json.dumps(
            {
                "inserted_rows": len(tuples),
                "model_version": args.model_version,
                "category": args.category,
                "running_style_feature_version": args.running_style_feature_version,
                "finish_position_version": args.finish_position_version,
                "window": [args.window_from, args.window_to],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
