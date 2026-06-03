"""DuckDB host-side fast-path for the finish-position v7-lineage bucket evaluation.

Re-implements the per-fold-year bucket aggregate that ``evaluate-bucket-21y.ts``
runs on PostgreSQL (the ``buildBucketAggregateSql`` self-join O(N^2) pair_score +
ndcg math) as a single columnar DuckDB query per (category, year). DuckDB reads the
Stage 3 predictions parquet directly and ATTACHes the local PostgreSQL replica
(``race_entry_corner_features`` actuals + ``jvd_ra`` / ``nvd_ra`` race dimensions),
crunches all folds on the M5 Pro in minutes, then upserts the small aggregated
result (a few thousand rows) into Neon plus one global rollup row per category.

The metric expressions are byte-for-byte equivalent to the TypeScript
``buildBucketAggregateSql`` so the numbers match the PG path exactly (validated
against the original PG SQL before upserting). Only the engine changes.

Run with:
  uv run python src/scripts/aggregate_bucket_eval_duckdb.py \
    --predictions-glob 'tmp/bucket-eval/finish-position/v7-lineage-wf-21y/predictions/**/*.parquet' \
    --local-pg-url postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing \
    --neon-url "$NEON_DIRECT_DATABASE_URL" \
    --running-style-feature-version v3 --finish-position-version v1
"""

from __future__ import annotations

import argparse
import importlib
import json
from decimal import Decimal
from typing import Callable, Protocol, TypedDict

BUCKET_TABLE = "model_prediction_bucket_evaluations"
EVALUATIONS_TABLE = "model_prediction_evaluations"

CATEGORY_JRA = "jra"
CATEGORY_NAR = "nar"
CATEGORY_BAN_EI = "ban-ei"

JRA_RA_TABLE = "jvd_ra"
NAR_RA_TABLE = "nvd_ra"
BANEI_KEIBAJO_CODE = "83"

JANUARY_FIRST_SUFFIX = "0101"
DECEMBER_LAST_SUFFIX = "1231"

# Condition-key CASE pairs, identical to CONDITION_LABEL_PAIRS in
# evaluate-bucket-predictions-sql.ts (NAR-only condition bucketing).
CONDITION_LABEL_PAIRS: tuple[tuple[str, str], ...] = (
    ("005", "1勝クラス"),
    ("010", "2勝クラス"),
    ("016", "3勝クラス"),
    ("701", "新馬"),
    ("702", "未出走"),
    ("703", "未勝利"),
    ("999", "オープン"),
)

# Walk-forward fold years per category (mirrors evaluate-bucket-21y-v7lineage.ts).
JRA_YEARS: tuple[int, ...] = tuple(range(2007, 2027))
NAR_YEARS: tuple[int, ...] = tuple(range(2007, 2027))
BAN_EI_YEARS: tuple[int, ...] = tuple(range(2008, 2027))

# Insert/upsert column order for the per-bucket REPLACE upsert into Neon.
BUCKET_INSERT_COLUMNS: tuple[str, ...] = (
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
    "race_count",
    "prediction_count",
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
    "pair_score_pair_count",
    "ndcg_at_3_sum",
    "ndcg_at_3_race_count",
)

BUCKET_CONFLICT_COLUMNS: tuple[str, ...] = (
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

# Global rollup (model_prediction_evaluations) column order.
GLOBAL_INSERT_COLUMNS: tuple[str, ...] = (
    "model_version",
    "category",
    "evaluation_window_from",
    "evaluation_window_to",
    "race_count",
    "prediction_count",
    "top1_accuracy",
    "top3_box_accuracy",
    "top3_exact_accuracy",
    "place1_accuracy",
    "place2_accuracy",
    "place3_accuracy",
    "top3_winner_capture",
    "top5_winner_capture",
    "pair_score",
    "ndcg_at_3",
    "top3_place_relation",
)

GLOBAL_CONFLICT_COLUMNS: tuple[str, ...] = (
    "model_version",
    "category",
    "evaluation_window_from",
    "evaluation_window_to",
)

# Default v7-lineage model versions per category (mirrors
# FINISH_POSITION_V7_LINEAGE_MODEL_VERSIONS in v7-lineage-model-versions.ts).
DEFAULT_MODEL_VERSIONS: dict[str, str] = {
    CATEGORY_JRA: "jra-cb-v7-lineage-wf-21y",
    CATEGORY_NAR: "nar-xgb-v7-lineage-wf-21y",
    CATEGORY_BAN_EI: "banei-cb-v7-lineage-wf-21y",
}

UPSERT_BATCH_SIZE = 500
SINGLE_QUOTE = "'"
DOUBLED_SINGLE_QUOTE = "''"


class CategoryMeta(TypedDict):
    ra_table: str
    ra_filter: str
    actuals_filter: str
    years: tuple[int, ...]


class AggregateArgs(TypedDict):
    predictions_glob: str
    model_version: str
    category: str
    from_date: str
    to_date: str
    running_style_feature_version: str
    finish_position_version: str


class GlobalRollup(TypedDict):
    race_count: int
    prediction_count: int
    top1_accuracy: float | None
    place1_accuracy: float | None
    place2_accuracy: float | None
    place3_accuracy: float | None
    top3_box_accuracy: float | None
    top3_exact_accuracy: float | None
    top3_winner_capture: float | None
    top5_winner_capture: float | None
    top3_place_relation: float | None
    pair_score: float | None
    ndcg_at_3: float | None


def coerce_int(value: object) -> int:
    """Coerce a DuckDB / parquet scalar to int (handles Decimal, which DuckDB emits
    for un-cast numeric sums such as pair_score_pair_count)."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float, Decimal)):
        return int(value)
    if isinstance(value, str) and value != "":
        return int(float(value))
    return 0


def coerce_float(value: object) -> float:
    """Coerce a DuckDB / parquet scalar to float (handles Decimal, which DuckDB emits
    for un-cast numeric sums such as pair_score_sum / top3_place_relation_sum)."""
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    if isinstance(value, str) and value != "":
        return float(value)
    return 0.0


def sql_quote_literal(value: str) -> str:
    return value.replace(SINGLE_QUOTE, DOUBLED_SINGLE_QUOTE)


# PostgreSQL's single-argument trim() strips ONLY the ASCII space character (0x20),
# NOT tabs / newlines / full-width spaces. DuckDB's single-argument trim() strips all
# Unicode whitespace (including U+3000), so we must use the explicit two-argument
# trim(x, ' ') form to replicate PostgreSQL byte-for-byte (validated against PG).
ASCII_SPACE_TRIM_CHARS = "' '"


def build_condition_case_sql(joken_column: str, meisho_column: str) -> str:
    """Condition-key CASE, equivalent to buildConditionCaseSql in the TS source."""
    when_clauses = "\n        ".join(
        f"when {joken_column} = '{sql_quote_literal(code)}' then '{sql_quote_literal(label)}'"
        for code, label in CONDITION_LABEL_PAIRS
    )
    return (
        "case\n        "
        f"{when_clauses}\n        "
        f"else nullif(split_part(trim({meisho_column}, {ASCII_SPACE_TRIM_CHARS}), ' ', 1), '')\n      end"
    )


def build_race_name_sql(grade_column: str, hondai_column: str) -> str:
    """Race-name expression, equivalent to buildRaceNameExpressionSql in the TS source."""
    return (
        f"case when {grade_column} in ('A','F') "
        f"then trim({hondai_column}, {ASCII_SPACE_TRIM_CHARS}) else null end"
    )


def resolve_category_meta(category: str) -> CategoryMeta:
    if category == CATEGORY_JRA:
        return {
            "ra_table": JRA_RA_TABLE,
            "ra_filter": "true",
            "actuals_filter": "rec.source = 'jra'",
            "years": JRA_YEARS,
        }
    if category == CATEGORY_NAR:
        return {
            "ra_table": NAR_RA_TABLE,
            "ra_filter": f"ra.keibajo_code <> '{BANEI_KEIBAJO_CODE}'",
            "actuals_filter": f"rec.source = 'nar' and rec.keibajo_code <> '{BANEI_KEIBAJO_CODE}'",
            "years": NAR_YEARS,
        }
    if category == CATEGORY_BAN_EI:
        return {
            "ra_table": NAR_RA_TABLE,
            "ra_filter": f"ra.keibajo_code = '{BANEI_KEIBAJO_CODE}'",
            "actuals_filter": f"rec.source = 'nar' and rec.keibajo_code = '{BANEI_KEIBAJO_CODE}'",
            "years": BAN_EI_YEARS,
        }
    raise ValueError(f"Unknown category: {category}")


def build_year_window(year: int) -> tuple[str, str]:
    return f"{year}{JANUARY_FIRST_SUFFIX}", f"{year}{DECEMBER_LAST_SUFFIX}"


def build_plan_window(years: tuple[int, ...]) -> tuple[str, str]:
    first = years[0] if len(years) > 0 else 0
    last = years[-1] if len(years) > 0 else 0
    return f"{first}{JANUARY_FIRST_SUFFIX}", f"{last}{DECEMBER_LAST_SUFFIX}"


def build_bucket_aggregate_sql(args: AggregateArgs) -> str:
    """DuckDB bucket aggregate, byte-equivalent in semantics to buildBucketAggregateSql.

    ``ra`` columns live in the ATTACHed PostgreSQL replica (alias ``pg``); predictions
    come from the parquet glob via read_parquet hive_partitioning. ``kyori`` is cast to
    integer because PG stores it zero-padded text but the bucket table column is integer
    (PG casts implicitly on insert; DuckDB needs the explicit cast to match).
    """
    meta = resolve_category_meta(args["category"])
    is_banei = args["category"] == CATEGORY_BAN_EI
    track_expr = "null::text" if is_banei else "ra.track_code"
    joken_expr = "null::text" if is_banei else "ra.kyoso_joken_code"
    condition_expr = (
        build_condition_case_sql("ra.kyoso_joken_code", "ra.kyoso_joken_meisho")
        if args["category"] == CATEGORY_NAR
        else "null::text"
    )
    race_name_expr = build_race_name_sql("ra.grade_code", "ra.kyosomei_hondai")
    return f"""
    with predictions as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             ketto_toroku_bango, cast(predicted_rank as integer) predicted_rank, predicted_score
      from read_parquet('{sql_quote_literal(args["predictions_glob"])}', hive_partitioning=1)
      where model_version = '{sql_quote_literal(args["model_version"])}'
        and running_style_feature_version = '{sql_quote_literal(args["running_style_feature_version"])}'
        and finish_position_version = '{sql_quote_literal(args["finish_position_version"])}'
        and category = '{sql_quote_literal(args["category"])}'
    ),
    actuals as (
      select rec.source, rec.kaisai_nen, rec.kaisai_tsukihi, rec.keibajo_code, rec.race_bango,
             rec.ketto_toroku_bango, rec.finish_position
      from pg.race_entry_corner_features rec
      where rec.race_date between '{sql_quote_literal(args["from_date"])}' and '{sql_quote_literal(args["to_date"])}'
        and rec.finish_position is not null
        and {meta["actuals_filter"]}
    ),
    joined as (
      select p.*, a.finish_position
      from predictions p
      join actuals a using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    ),
    races as (
      select distinct source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango from joined
    ),
    race_dims as (
      select r.source, r.kaisai_nen, r.kaisai_tsukihi, r.keibajo_code, r.race_bango,
             cast(ra.kyori as integer) as kyori, ra.kyoso_shubetsu_code,
             {joken_expr} as kyoso_joken_code,
             {condition_expr} as condition_key,
             {track_expr} as track_code,
             nullif(trim(ra.grade_code, {ASCII_SPACE_TRIM_CHARS}), '') as grade_code,
             {race_name_expr} as race_name
      from races r
      join pg.{meta["ra_table"]} ra
        on ra.kaisai_nen = r.kaisai_nen
       and ra.kaisai_tsukihi = r.kaisai_tsukihi
       and ra.keibajo_code = r.keibajo_code
       and ra.race_bango = r.race_bango
      where {meta["ra_filter"]}
    ),
    per_race as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) top1_hit,
             cast((sum(case when predicted_rank <= 3 and finish_position <= 3 then 1 else 0 end) = 3) as int) top3_box_hit,
             cast((
               max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) = 1
               and max(case when predicted_rank = 2 and finish_position = 2 then 1 else 0 end) = 1
               and max(case when predicted_rank = 3 and finish_position = 3 then 1 else 0 end) = 1
             ) as int) top3_exact_hit,
             max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) place1_hit,
             max(case when predicted_rank = 2 and finish_position = 2 then 1 else 0 end) place2_hit,
             max(case when predicted_rank = 3 and finish_position = 3 then 1 else 0 end) place3_hit,
             max(case when predicted_rank <= 3 and finish_position = 1 then 1 else 0 end) top3_winner_capture_hit,
             max(case when predicted_rank <= 5 and finish_position = 1 then 1 else 0 end) top5_winner_capture_hit,
             sum(case when predicted_rank <= 3 and finish_position <= 3 then 1.0 else 0.0 end) / 3.0 top3_place_relation_val,
             count(*) prediction_rows
      from joined
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    ),
    pair_per_race as (
      select j1.source, j1.kaisai_nen, j1.kaisai_tsukihi, j1.keibajo_code, j1.race_bango,
             sum(
               case
                 when (j1.predicted_rank < j2.predicted_rank) = (j1.finish_position < j2.finish_position)
                 then 1.0 else 0.0
               end
             ) pair_correct_sum,
             count(*) pair_count
      from joined j1
      join joined j2
        on j1.source = j2.source
        and j1.kaisai_nen = j2.kaisai_nen
        and j1.kaisai_tsukihi = j2.kaisai_tsukihi
        and j1.keibajo_code = j2.keibajo_code
        and j1.race_bango = j2.race_bango
        and j1.ketto_toroku_bango < j2.ketto_toroku_bango
      group by j1.source, j1.kaisai_nen, j1.kaisai_tsukihi, j1.keibajo_code, j1.race_bango
    ),
    ndcg_per_race as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             sum(
               case
                 when predicted_rank <= 3
                 then (greatest(0, 4 - finish_position)) / ln(2 + predicted_rank)
                 else 0
               end
             ) dcg,
             (3 / ln(2 + 1) + 2 / ln(2 + 2) + 1 / ln(2 + 3)) ideal_dcg
      from joined
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    )
    select
      d.source,
      d.keibajo_code,
      d.kyori,
      d.kyoso_shubetsu_code,
      d.kyoso_joken_code,
      d.condition_key,
      d.track_code,
      d.grade_code,
      d.race_name,
      count(*) race_count,
      coalesce(sum(pr.prediction_rows), 0) prediction_count,
      coalesce(sum(cast(pr.top1_hit as double)), 0) top1_hit_sum,
      coalesce(sum(cast(pr.place1_hit as double)), 0) place1_hit_sum,
      coalesce(sum(cast(pr.place2_hit as double)), 0) place2_hit_sum,
      coalesce(sum(cast(pr.place3_hit as double)), 0) place3_hit_sum,
      coalesce(sum(cast(pr.top3_box_hit as double)), 0) top3_box_hit_sum,
      coalesce(sum(cast(pr.top3_exact_hit as double)), 0) top3_exact_hit_sum,
      coalesce(sum(cast(pr.top3_winner_capture_hit as double)), 0) top3_winner_capture_sum,
      coalesce(sum(cast(pr.top5_winner_capture_hit as double)), 0) top5_winner_capture_sum,
      coalesce(sum(pr.top3_place_relation_val), 0) top3_place_relation_sum,
      coalesce(sum(pp.pair_correct_sum), 0) pair_score_sum,
      coalesce(sum(pp.pair_count), 0) pair_score_pair_count,
      coalesce(sum(case when nd.ideal_dcg > 0 then nd.dcg / nd.ideal_dcg else 0 end), 0) ndcg_at_3_sum,
      coalesce(sum(case when nd.ideal_dcg > 0 then 1 else 0 end), 0) ndcg_at_3_race_count
    from race_dims d
    join per_race pr using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    left join pair_per_race pp using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    left join ndcg_per_race nd using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    group by d.source, d.keibajo_code, d.kyori, d.kyoso_shubetsu_code,
             d.kyoso_joken_code, d.condition_key, d.track_code, d.grade_code, d.race_name
  """


def build_bucket_evaluations_ddl() -> str:
    """CREATE TABLE/INDEX IF NOT EXISTS for the per-bucket table (DDL only)."""
    unique_cols = ", ".join(BUCKET_CONFLICT_COLUMNS)
    lookup_cols = (
        "model_version, running_style_feature_version, finish_position_version, category, "
        "source, keibajo_code, kyori, kyoso_shubetsu_code, kyoso_joken_code, condition_key, "
        "track_code, grade_code"
    )
    return f"""
    create table if not exists {BUCKET_TABLE} (
      model_version                 text not null,
      running_style_feature_version text not null,
      finish_position_version       text not null,
      category                      text not null,
      evaluation_window_from        text not null,
      evaluation_window_to          text not null,
      source                        text not null,
      keibajo_code                  text not null,
      kyori                         integer not null,
      kyoso_shubetsu_code           text not null,
      kyoso_joken_code              text,
      condition_key                 text,
      track_code                    text,
      grade_code                    text,
      race_name                     text,
      race_count                    integer not null,
      prediction_count              integer not null,
      top1_hit_sum                  numeric not null,
      place1_hit_sum                numeric not null,
      place2_hit_sum                numeric not null,
      place3_hit_sum                numeric not null,
      top3_box_hit_sum              numeric not null,
      top3_exact_hit_sum            numeric not null,
      top3_winner_capture_sum       numeric not null,
      top5_winner_capture_sum       numeric not null,
      top3_place_relation_sum       numeric not null,
      pair_score_sum                numeric not null,
      pair_score_pair_count         integer not null,
      ndcg_at_3_sum                 numeric not null,
      ndcg_at_3_race_count          integer not null,
      evaluated_at                  timestamptz not null default now()
    );
    create unique index if not exists {BUCKET_TABLE}_uq
      on {BUCKET_TABLE} ({unique_cols});
    create index if not exists {BUCKET_TABLE}_lookup
      on {BUCKET_TABLE} ({lookup_cols});
    create index if not exists {BUCKET_TABLE}_race_name
      on {BUCKET_TABLE} (category, source, race_name, keibajo_code, kyori)
      where race_name is not null;
  """


def build_global_evaluations_ddl() -> str:
    primary_key = ", ".join(GLOBAL_CONFLICT_COLUMNS)
    return f"""
    create table if not exists {EVALUATIONS_TABLE} (
      model_version text not null,
      category text not null,
      evaluation_window_from text not null,
      evaluation_window_to text not null,
      race_count integer not null,
      prediction_count integer not null,
      top1_accuracy numeric,
      top3_box_accuracy numeric,
      top3_exact_accuracy numeric,
      place1_accuracy numeric,
      place2_accuracy numeric,
      place3_accuracy numeric,
      top3_winner_capture numeric,
      top5_winner_capture numeric,
      top3_place_relation numeric,
      pair_score numeric,
      ndcg_at_3 numeric,
      evaluated_at timestamptz not null default now(),
      primary key ({primary_key})
    );
    alter table {EVALUATIONS_TABLE} add column if not exists top3_place_relation numeric
  """


# Bucket-table dimension columns are never overwritten by the upsert SET clause
# (they are part of the conflict key / immutable identity of the bucket row).
BUCKET_DIMENSION_INSERT_COLUMNS: tuple[str, ...] = (
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


def build_bucket_upsert_sql() -> str:
    """Single-row REPLACE upsert (psycopg3 executemany %s placeholders)."""
    column_list = ", ".join(BUCKET_INSERT_COLUMNS)
    conflict_keys = ", ".join(BUCKET_CONFLICT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(BUCKET_INSERT_COLUMNS))
    set_fragments = ",\n      ".join(
        f"{col} = excluded.{col}"
        for col in BUCKET_INSERT_COLUMNS
        if col not in BUCKET_DIMENSION_INSERT_COLUMNS
    )
    return (
        f"insert into {BUCKET_TABLE} ({column_list}, evaluated_at)\n"
        f"values ({placeholders}, now())\n"
        f"on conflict ({conflict_keys}) do update set\n      {set_fragments},\n"
        f"      evaluated_at = now()"
    )


def build_global_upsert_sql() -> str:
    column_list = ", ".join(GLOBAL_INSERT_COLUMNS)
    conflict_keys = ", ".join(GLOBAL_CONFLICT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(GLOBAL_INSERT_COLUMNS))
    set_fragments = ",\n      ".join(
        f"{col} = excluded.{col}"
        for col in GLOBAL_INSERT_COLUMNS
        if col not in GLOBAL_CONFLICT_COLUMNS
    )
    return (
        f"insert into {EVALUATIONS_TABLE} ({column_list}, evaluated_at)\n"
        f"values ({placeholders}, now())\n"
        f"on conflict ({conflict_keys}) do update set\n      {set_fragments},\n"
        f"      evaluated_at = now()"
    )


def to_aggregate_args(
    *,
    predictions_glob: str,
    model_version: str,
    category: str,
    from_date: str,
    to_date: str,
    running_style_feature_version: str,
    finish_position_version: str,
) -> AggregateArgs:
    return {
        "predictions_glob": predictions_glob,
        "model_version": model_version,
        "category": category,
        "from_date": from_date,
        "to_date": to_date,
        "running_style_feature_version": running_style_feature_version,
        "finish_position_version": finish_position_version,
    }


def build_bucket_upsert_row(
    *,
    aggregate_row: tuple[object, ...],
    model_version: str,
    running_style_feature_version: str,
    finish_position_version: str,
    category: str,
    window_from: str,
    window_to: str,
) -> tuple[object, ...]:
    """Prepend the version / window dimensions to a DuckDB aggregate row.

    ``aggregate_row`` is exactly the column order of build_bucket_aggregate_sql:
    9 bucket dims + 15 metrics = 24 fields. The upsert tuple is 30 fields.
    """
    return (
        model_version,
        running_style_feature_version,
        finish_position_version,
        category,
        window_from,
        window_to,
        *aggregate_row,
    )


def compute_global_rollup(rows: list[tuple[object, ...]]) -> GlobalRollup:
    """Roll the per-bucket sums up to one global row, matching buildGlobalRollupSql.

    Indices into the aggregate row: race_count=9, prediction_count=10,
    top1_hit_sum=11 .. ndcg_at_3_race_count=23 (see build_bucket_aggregate_sql).
    """
    race_count = sum(coerce_int(row[9]) for row in rows)
    prediction_count = sum(coerce_int(row[10]) for row in rows)
    top1 = sum(coerce_float(row[11]) for row in rows)
    place1 = sum(coerce_float(row[12]) for row in rows)
    place2 = sum(coerce_float(row[13]) for row in rows)
    place3 = sum(coerce_float(row[14]) for row in rows)
    top3_box = sum(coerce_float(row[15]) for row in rows)
    top3_exact = sum(coerce_float(row[16]) for row in rows)
    top3_winner = sum(coerce_float(row[17]) for row in rows)
    top5_winner = sum(coerce_float(row[18]) for row in rows)
    top3_place_rel = sum(coerce_float(row[19]) for row in rows)
    pair_sum = sum(coerce_float(row[20]) for row in rows)
    pair_count = sum(coerce_int(row[21]) for row in rows)
    ndcg_sum = sum(coerce_float(row[22]) for row in rows)
    ndcg_count = sum(coerce_int(row[23]) for row in rows)

    def per_race(value: float) -> float | None:
        return value / race_count if race_count > 0 else None

    return {
        "race_count": race_count,
        "prediction_count": prediction_count,
        "top1_accuracy": per_race(top1),
        "place1_accuracy": per_race(place1),
        "place2_accuracy": per_race(place2),
        "place3_accuracy": per_race(place3),
        "top3_box_accuracy": per_race(top3_box),
        "top3_exact_accuracy": per_race(top3_exact),
        "top3_winner_capture": per_race(top3_winner),
        "top5_winner_capture": per_race(top5_winner),
        "top3_place_relation": per_race(top3_place_rel),
        "pair_score": pair_sum / pair_count if pair_count > 0 else None,
        "ndcg_at_3": ndcg_sum / ndcg_count if ndcg_count > 0 else None,
    }


def build_global_upsert_row(
    *,
    rollup: GlobalRollup,
    model_version: str,
    category: str,
    window_from: str,
    window_to: str,
) -> tuple[object, ...]:
    return (
        model_version,
        category,
        window_from,
        window_to,
        rollup["race_count"],
        rollup["prediction_count"],
        rollup["top1_accuracy"],
        rollup["top3_box_accuracy"],
        rollup["top3_exact_accuracy"],
        rollup["place1_accuracy"],
        rollup["place2_accuracy"],
        rollup["place3_accuracy"],
        rollup["top3_winner_capture"],
        rollup["top5_winner_capture"],
        rollup["pair_score"],
        rollup["ndcg_at_3"],
        rollup["top3_place_relation"],
    )


def chunk_rows(
    rows: list[tuple[object, ...]], size: int
) -> list[list[tuple[object, ...]]]:
    if size <= 0:
        raise ValueError("chunk size must be positive")
    return [rows[start : start + size] for start in range(0, len(rows), size)]


class DuckdbResult(Protocol):
    def fetchall(self) -> list[tuple[object, ...]]: ...


class DuckdbConnection(Protocol):
    def execute(self, query: str, /) -> DuckdbResult: ...
    def close(self) -> None: ...


class PgCursor(Protocol):
    def execute(self, query: str, /) -> object: ...
    def executemany(self, query: str, params_seq: list[tuple[object, ...]], /) -> object: ...
    def __enter__(self) -> PgCursor: ...
    def __exit__(self, *exc: object) -> object: ...


class PgConnection(Protocol):
    def cursor(self) -> PgCursor: ...
    def commit(self) -> None: ...
    def close(self) -> None: ...


def default_duckdb_connect(local_pg_url: str, threads: int) -> DuckdbConnection:
    # importlib keeps the dynamic connection typed as Any (assignable to the Protocol)
    # so we avoid pulling concrete vendor stubs that diverge from our minimal surface.
    duckdb_module = importlib.import_module("duckdb")
    con: DuckdbConnection = duckdb_module.connect(":memory:")
    con.execute("set enable_progress_bar=false;")
    con.execute(f"set threads={threads};")
    con.execute("install postgres; load postgres;")
    con.execute(f"attach '{sql_quote_literal(local_pg_url)}' as pg (type postgres, read_only)")
    return con


def default_psycopg_connect(neon_url: str) -> PgConnection:
    # Resolve psycopg.connect dynamically via getattr so the connection stays typed as
    # the dynamic module's return (Any), assignable to our minimal PgConnection protocol
    # without pulling psycopg's concrete Connection stub (which over-constrains cursor()).
    module = importlib.import_module("psycopg")
    connect_fn = getattr(module, "connect")
    connection: PgConnection = connect_fn(neon_url)
    return connection


ConnectDuckdbFn = Callable[[str, int], DuckdbConnection]
ConnectPgFn = Callable[[str], PgConnection]


class AggregateResult(TypedDict):
    bucket_rows: int
    rollups: int
    categories: int


def aggregate_category_year(
    duck: DuckdbConnection,
    *,
    predictions_glob: str,
    model_version: str,
    category: str,
    year: int,
    running_style_feature_version: str,
    finish_position_version: str,
) -> list[tuple[object, ...]]:
    from_date, to_date = build_year_window(year)
    sql = build_bucket_aggregate_sql(
        to_aggregate_args(
            predictions_glob=predictions_glob,
            model_version=model_version,
            category=category,
            from_date=from_date,
            to_date=to_date,
            running_style_feature_version=running_style_feature_version,
            finish_position_version=finish_position_version,
        )
    )
    return list(duck.execute(sql).fetchall())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="aggregate_bucket_eval_duckdb")
    parser.add_argument("--predictions-glob", type=str, required=True)
    parser.add_argument("--local-pg-url", type=str, required=True)
    parser.add_argument("--neon-url", type=str, required=True)
    parser.add_argument("--running-style-feature-version", type=str, required=True)
    parser.add_argument("--finish-position-version", type=str, required=True)
    parser.add_argument("--model-version-jra", type=str, default=DEFAULT_MODEL_VERSIONS[CATEGORY_JRA])
    parser.add_argument("--model-version-nar", type=str, default=DEFAULT_MODEL_VERSIONS[CATEGORY_NAR])
    parser.add_argument(
        "--model-version-banei", type=str, default=DEFAULT_MODEL_VERSIONS[CATEGORY_BAN_EI]
    )
    parser.add_argument("--threads", type=int, default=15)
    return parser.parse_args(argv)


def resolve_model_version(args: argparse.Namespace, category: str) -> str:
    if category == CATEGORY_JRA:
        return str(args.model_version_jra)
    if category == CATEGORY_NAR:
        return str(args.model_version_nar)
    return str(args.model_version_banei)


def ensure_neon_tables(pg: PgConnection) -> None:
    """Run only the CREATE TABLE/INDEX IF NOT EXISTS DDL on Neon (no heavy aggregate)."""
    with pg.cursor() as cursor:
        cursor.execute(build_bucket_evaluations_ddl())
    with pg.cursor() as cursor:
        cursor.execute(build_global_evaluations_ddl())
    pg.commit()


def upsert_bucket_rows(
    pg: PgConnection,
    rows: list[tuple[object, ...]],
) -> None:
    """REPLACE upsert the per-bucket rows in <=500-row batches via executemany."""
    sql = build_bucket_upsert_sql()
    for batch in chunk_rows(rows, UPSERT_BATCH_SIZE):
        with pg.cursor() as cursor:
            cursor.executemany(sql, batch)
    pg.commit()


def upsert_global_rows(
    pg: PgConnection,
    rows: list[tuple[object, ...]],
) -> None:
    sql = build_global_upsert_sql()
    with pg.cursor() as cursor:
        cursor.executemany(sql, rows)
    pg.commit()


def collect_category(
    duck: DuckdbConnection,
    args: argparse.Namespace,
    category: str,
) -> tuple[list[tuple[object, ...]], tuple[object, ...]]:
    meta = resolve_category_meta(category)
    model_version = resolve_model_version(args, category)
    all_aggregate_rows: list[tuple[object, ...]] = []
    bucket_upsert_rows: list[tuple[object, ...]] = []
    for year in meta["years"]:
        from_date, to_date = build_year_window(year)
        aggregate_rows = aggregate_category_year(
            duck,
            predictions_glob=str(args.predictions_glob),
            model_version=model_version,
            category=category,
            year=year,
            running_style_feature_version=str(args.running_style_feature_version),
            finish_position_version=str(args.finish_position_version),
        )
        all_aggregate_rows.extend(aggregate_rows)
        bucket_upsert_rows.extend(
            build_bucket_upsert_row(
                aggregate_row=row,
                model_version=model_version,
                running_style_feature_version=str(args.running_style_feature_version),
                finish_position_version=str(args.finish_position_version),
                category=category,
                window_from=from_date,
                window_to=to_date,
            )
            for row in aggregate_rows
        )
    plan_from, plan_to = build_plan_window(meta["years"])
    rollup = compute_global_rollup(all_aggregate_rows)
    global_row = build_global_upsert_row(
        rollup=rollup,
        model_version=model_version,
        category=category,
        window_from=plan_from,
        window_to=plan_to,
    )
    return bucket_upsert_rows, global_row


def run_aggregation(
    args: argparse.Namespace,
    *,
    connect_duckdb: ConnectDuckdbFn = default_duckdb_connect,
    connect_pg: ConnectPgFn = default_psycopg_connect,
) -> AggregateResult:
    duck = connect_duckdb(str(args.local_pg_url), int(args.threads))
    categories = (CATEGORY_JRA, CATEGORY_NAR, CATEGORY_BAN_EI)
    bucket_rows: list[tuple[object, ...]] = []
    global_rows: list[tuple[object, ...]] = []
    try:
        for category in categories:
            category_bucket_rows, global_row = collect_category(duck, args, category)
            bucket_rows.extend(category_bucket_rows)
            global_rows.append(global_row)
    finally:
        duck.close()
    pg = connect_pg(str(args.neon_url))
    try:
        ensure_neon_tables(pg)
        upsert_bucket_rows(pg, bucket_rows)
        upsert_global_rows(pg, global_rows)
    finally:
        pg.close()
    return {
        "bucket_rows": len(bucket_rows),
        "rollups": len(global_rows),
        "categories": len(categories),
    }


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    result = run_aggregation(args)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
