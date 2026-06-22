"""Insert or update one or more rows in `running_style_model_bucket_evaluations` from a metrics JSON.

Run with:
  uv run python src/scripts/insert_running_style_bucket_evaluation_row.py \
    --metrics-json tmp/bucket-eval/running-style/row.json \
    --model-version jra-running-style-ens-lgbm-trans-v1.3 \
    --running-style-feature-version v1 \
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
from typing import Callable, Protocol, TypedDict, cast

BUCKET_TABLE = "running_style_model_bucket_evaluations"

CM_NUMERIC_COLUMNS: tuple[str, ...] = (
    "cm_actual_nige_pred_nige_count",
    "cm_actual_nige_pred_senkou_count",
    "cm_actual_nige_pred_sashi_count",
    "cm_actual_nige_pred_oikomi_count",
    "cm_actual_senkou_pred_nige_count",
    "cm_actual_senkou_pred_senkou_count",
    "cm_actual_senkou_pred_sashi_count",
    "cm_actual_senkou_pred_oikomi_count",
    "cm_actual_sashi_pred_nige_count",
    "cm_actual_sashi_pred_senkou_count",
    "cm_actual_sashi_pred_sashi_count",
    "cm_actual_sashi_pred_oikomi_count",
    "cm_actual_oikomi_pred_nige_count",
    "cm_actual_oikomi_pred_senkou_count",
    "cm_actual_oikomi_pred_sashi_count",
    "cm_actual_oikomi_pred_oikomi_count",
)

LOG_LOSS_SUM_COLUMNS: tuple[str, ...] = (
    "log_loss_nige_sum",
    "log_loss_senkou_sum",
    "log_loss_sashi_sum",
    "log_loss_oikomi_sum",
)

LOG_LOSS_COUNT_COLUMNS: tuple[str, ...] = (
    "log_loss_nige_count",
    "log_loss_senkou_count",
    "log_loss_sashi_count",
    "log_loss_oikomi_count",
)

TOP2_COLUMN: str = "top2_hit_count"

INT_BASE_COLUMNS: tuple[str, ...] = (
    "race_count",
    "prediction_count",
)

DIMENSION_COLUMNS: tuple[str, ...] = (
    "model_version",
    "running_style_feature_version",
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
    cm_actual_nige_pred_nige_count: int
    cm_actual_nige_pred_senkou_count: int
    cm_actual_nige_pred_sashi_count: int
    cm_actual_nige_pred_oikomi_count: int
    cm_actual_senkou_pred_nige_count: int
    cm_actual_senkou_pred_senkou_count: int
    cm_actual_senkou_pred_sashi_count: int
    cm_actual_senkou_pred_oikomi_count: int
    cm_actual_sashi_pred_nige_count: int
    cm_actual_sashi_pred_senkou_count: int
    cm_actual_sashi_pred_sashi_count: int
    cm_actual_sashi_pred_oikomi_count: int
    cm_actual_oikomi_pred_nige_count: int
    cm_actual_oikomi_pred_senkou_count: int
    cm_actual_oikomi_pred_sashi_count: int
    cm_actual_oikomi_pred_oikomi_count: int
    log_loss_nige_sum: float
    log_loss_nige_count: int
    log_loss_senkou_sum: float
    log_loss_senkou_count: int
    log_loss_sashi_sum: float
    log_loss_sashi_count: int
    log_loss_oikomi_sum: float
    log_loss_oikomi_count: int
    top2_hit_count: int


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="insert_running_style_bucket_evaluation_row")
    parser.add_argument("--pg-url", type=str, required=True)
    parser.add_argument("--metrics-json", type=Path, required=True)
    parser.add_argument("--model-version", type=str, required=True)
    parser.add_argument("--running-style-feature-version", type=str, required=True)
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
        cm_actual_nige_pred_nige_count=to_int(raw_row.get("cm_actual_nige_pred_nige_count")),
        cm_actual_nige_pred_senkou_count=to_int(raw_row.get("cm_actual_nige_pred_senkou_count")),
        cm_actual_nige_pred_sashi_count=to_int(raw_row.get("cm_actual_nige_pred_sashi_count")),
        cm_actual_nige_pred_oikomi_count=to_int(raw_row.get("cm_actual_nige_pred_oikomi_count")),
        cm_actual_senkou_pred_nige_count=to_int(raw_row.get("cm_actual_senkou_pred_nige_count")),
        cm_actual_senkou_pred_senkou_count=to_int(
            raw_row.get("cm_actual_senkou_pred_senkou_count")
        ),
        cm_actual_senkou_pred_sashi_count=to_int(raw_row.get("cm_actual_senkou_pred_sashi_count")),
        cm_actual_senkou_pred_oikomi_count=to_int(
            raw_row.get("cm_actual_senkou_pred_oikomi_count")
        ),
        cm_actual_sashi_pred_nige_count=to_int(raw_row.get("cm_actual_sashi_pred_nige_count")),
        cm_actual_sashi_pred_senkou_count=to_int(raw_row.get("cm_actual_sashi_pred_senkou_count")),
        cm_actual_sashi_pred_sashi_count=to_int(raw_row.get("cm_actual_sashi_pred_sashi_count")),
        cm_actual_sashi_pred_oikomi_count=to_int(raw_row.get("cm_actual_sashi_pred_oikomi_count")),
        cm_actual_oikomi_pred_nige_count=to_int(raw_row.get("cm_actual_oikomi_pred_nige_count")),
        cm_actual_oikomi_pred_senkou_count=to_int(
            raw_row.get("cm_actual_oikomi_pred_senkou_count")
        ),
        cm_actual_oikomi_pred_sashi_count=to_int(raw_row.get("cm_actual_oikomi_pred_sashi_count")),
        cm_actual_oikomi_pred_oikomi_count=to_int(
            raw_row.get("cm_actual_oikomi_pred_oikomi_count")
        ),
        log_loss_nige_sum=to_float(raw_row.get("log_loss_nige_sum")),
        log_loss_nige_count=to_int(raw_row.get("log_loss_nige_count")),
        log_loss_senkou_sum=to_float(raw_row.get("log_loss_senkou_sum")),
        log_loss_senkou_count=to_int(raw_row.get("log_loss_senkou_count")),
        log_loss_sashi_sum=to_float(raw_row.get("log_loss_sashi_sum")),
        log_loss_sashi_count=to_int(raw_row.get("log_loss_sashi_count")),
        log_loss_oikomi_sum=to_float(raw_row.get("log_loss_oikomi_sum")),
        log_loss_oikomi_count=to_int(raw_row.get("log_loss_oikomi_count")),
        top2_hit_count=to_int(raw_row.get("top2_hit_count")),
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


def build_metric_columns() -> list[str]:
    return [
        *INT_BASE_COLUMNS,
        *CM_NUMERIC_COLUMNS,
        *LOG_LOSS_SUM_COLUMNS,
        *LOG_LOSS_COUNT_COLUMNS,
        TOP2_COLUMN,
    ]


def build_insert_columns() -> list[str]:
    return list(DIMENSION_COLUMNS) + build_metric_columns()


def build_additive_set_clause(column: str) -> str:
    return f"{column} = excluded.{column} + {BUCKET_TABLE}.{column}"


def build_upsert_sql() -> str:
    metric_columns = build_metric_columns()
    set_fragments = ",\n  ".join(
        [build_additive_set_clause(col) for col in metric_columns] + ["evaluated_at = now()"]
    )
    conflict_keys = ", ".join(CONFLICT_COLUMNS)
    column_list = ", ".join(build_insert_columns())
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
    category: str,
    window_from: str,
    window_to: str,
) -> tuple[object, ...]:
    return (
        model_version,
        running_style_feature_version,
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
        row["cm_actual_nige_pred_nige_count"],
        row["cm_actual_nige_pred_senkou_count"],
        row["cm_actual_nige_pred_sashi_count"],
        row["cm_actual_nige_pred_oikomi_count"],
        row["cm_actual_senkou_pred_nige_count"],
        row["cm_actual_senkou_pred_senkou_count"],
        row["cm_actual_senkou_pred_sashi_count"],
        row["cm_actual_senkou_pred_oikomi_count"],
        row["cm_actual_sashi_pred_nige_count"],
        row["cm_actual_sashi_pred_senkou_count"],
        row["cm_actual_sashi_pred_sashi_count"],
        row["cm_actual_sashi_pred_oikomi_count"],
        row["cm_actual_oikomi_pred_nige_count"],
        row["cm_actual_oikomi_pred_senkou_count"],
        row["cm_actual_oikomi_pred_sashi_count"],
        row["cm_actual_oikomi_pred_oikomi_count"],
        row["log_loss_nige_sum"],
        row["log_loss_senkou_sum"],
        row["log_loss_sashi_sum"],
        row["log_loss_oikomi_sum"],
        row["log_loss_nige_count"],
        row["log_loss_senkou_count"],
        row["log_loss_sashi_count"],
        row["log_loss_oikomi_count"],
        row["top2_hit_count"],
    )


class PgCursor(Protocol):
    def executemany(self, query: str, params_seq: list[tuple[object, ...]], /) -> None: ...


class PgConnection(Protocol):
    def cursor(self) -> AbstractContextManager[PgCursor]: ...
    def commit(self) -> None: ...


def _default_psycopg_connect(pg_url: str) -> AbstractContextManager[PgConnection]:
    module = importlib.import_module("psycopg")
    return cast(AbstractContextManager[PgConnection], module.connect(pg_url))


def _default_execute_values() -> Callable[..., None]:
    def _execute_values(
        cursor: PgCursor, sql: str, args: list[tuple[object, ...]], *, template: str
    ) -> None:
        row_sql = sql.replace("VALUES %s", f"VALUES {template}")
        cursor.executemany(row_sql, args)  # type: ignore[attr-defined]

    return _execute_values


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
                "window": [args.window_from, args.window_to],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
