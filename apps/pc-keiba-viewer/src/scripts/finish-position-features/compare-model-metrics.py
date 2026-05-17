#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
"""Compute and compare evaluation metrics for multiple prediction JSONL files
against a shared actuals CSV/parquet, using DuckDB in-process for speed.

Mirrors the metric definitions in
src/scripts/finish-position-features/evaluate-predictions-sql.ts but avoids
the PostgreSQL hop and the per-race self-join over a remote connection.

Run with:
  .venv/bin/python src/scripts/finish-position-features/compare-model-metrics.py \
    --actuals tmp/finish-position-eval/actuals-2024-2025.csv \
    --jsonl name=v1:tmp/.../predictions-v1/lambdarank/2024-2025.jsonl \
    --jsonl name=v2:tmp/.../predictions-v2/lambdarank/2024-2025.jsonl \
    --jsonl name=ensemble:tmp/.../predictions-v2/ensemble/2024-2025.jsonl
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

METRIC_SQL = """
with predictions as (
  select race_id, ketto_toroku_bango, predicted_rank
  from read_json_auto('{jsonl_path}')
),
actuals_full as (
  select race_id, ketto_toroku_bango, cast(finish_position as int) as finish_position
  from read_csv_auto('{actuals_path}', header=true)
  where finish_position is not null
),
joined as (
  select p.race_id, p.ketto_toroku_bango, p.predicted_rank, a.finish_position
  from predictions p
  join actuals_full a using (race_id, ketto_toroku_bango)
),
per_race as (
  select race_id,
    count(*) as runner_count,
    max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) as top1_hit,
    max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) as place1_hit,
    max(case when predicted_rank = 2 and finish_position = 2 then 1 else 0 end) as place2_hit,
    max(case when predicted_rank = 3 and finish_position = 3 then 1 else 0 end) as place3_hit,
    (
      sum(case when predicted_rank <= 3 and finish_position <= 3 then 1 else 0 end) = 3
    )::int as top3_box_hit,
    (
      max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) = 1
      and max(case when predicted_rank = 2 and finish_position = 2 then 1 else 0 end) = 1
      and max(case when predicted_rank = 3 and finish_position = 3 then 1 else 0 end) = 1
    )::int as top3_exact_hit,
    max(case when predicted_rank <= 3 and finish_position = 1 then 1 else 0 end) as top3_winner_capture,
    sum(case when predicted_rank <= 3 and finish_position <= 3 then 1.0 else 0.0 end) / 3.0
      as top3_place_relation,
    sum(case when predicted_rank <= 3 then (greatest(0, 4 - finish_position)) / ln(2 + predicted_rank) else 0 end)
      as dcg,
    (3 / ln(2 + 1) + 2 / ln(2 + 2) + 1 / ln(2 + 3)) as ideal_dcg
  from joined
  group by race_id
),
pair_per_race as (
  select j1.race_id,
    avg(
      case when (j1.predicted_rank < j2.predicted_rank) = (j1.finish_position < j2.finish_position)
           then 1.0 else 0.0 end
    ) as pair_correct
  from joined j1
  join joined j2
    on j1.race_id = j2.race_id and j1.ketto_toroku_bango < j2.ketto_toroku_bango
  group by j1.race_id
)
select
  (select count(*) from per_race) as race_count,
  (select count(*) from joined) as prediction_count,
  (select avg(top1_hit::double) from per_race) as top1_accuracy,
  (select avg(place1_hit::double) from per_race) as place1_accuracy,
  (select avg(place2_hit::double) from per_race) as place2_accuracy,
  (select avg(place3_hit::double) from per_race) as place3_accuracy,
  (select avg(top3_box_hit::double) from per_race) as top3_box_accuracy,
  (select avg(top3_exact_hit::double) from per_race) as top3_exact_accuracy,
  (select avg(top3_winner_capture::double) from per_race) as top3_winner_capture,
  (select avg(top3_place_relation) from per_race) as top3_place_relation,
  (select avg(case when ideal_dcg > 0 then dcg / ideal_dcg else null end) from per_race)
    as ndcg_at_3,
  (select avg(pair_correct) from pair_per_race) as pair_score
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="compare_model_metrics")
    parser.add_argument("--actuals", type=Path, required=True)
    parser.add_argument(
        "--jsonl",
        action="append",
        required=True,
        help="Format: name=label:/path/to/predictions.jsonl",
    )
    parser.add_argument("--output", type=Path, default=None)
    return parser.parse_args()


def parse_named_jsonl(raw: str) -> tuple[str, Path]:
    if not raw.startswith("name="):
        raise ValueError(f"--jsonl entry must start with name=label: prefix; got {raw}")
    after = raw[len("name="):]
    label, _, path_str = after.partition(":")
    if not label or not path_str:
        raise ValueError(f"--jsonl entry malformed: {raw}")
    return label, Path(path_str)


def run_metrics(con: duckdb.DuckDBPyConnection, actuals: Path, jsonl: Path) -> dict[str, object]:
    sql = METRIC_SQL.format(jsonl_path=jsonl.as_posix(), actuals_path=actuals.as_posix())
    row = con.execute(sql).fetchone()
    if row is None:
        raise RuntimeError("metric query returned no rows")
    keys = (
        "race_count",
        "prediction_count",
        "top1_accuracy",
        "place1_accuracy",
        "place2_accuracy",
        "place3_accuracy",
        "top3_box_accuracy",
        "top3_exact_accuracy",
        "top3_winner_capture",
        "top3_place_relation",
        "ndcg_at_3",
        "pair_score",
    )
    return {key: row[idx] for idx, key in enumerate(keys)}


def main() -> None:
    args = parse_args()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA enable_object_cache=true")
    results: list[dict[str, object]] = []
    for raw in args.jsonl:
        label, path = parse_named_jsonl(raw)
        metrics = run_metrics(con, args.actuals, path)
        results.append({"label": label, "jsonl": path.as_posix(), **metrics})
    payload = {"results": results}
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
