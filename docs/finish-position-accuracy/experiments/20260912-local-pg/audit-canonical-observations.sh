#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(git -C "$ROOT" rev-parse --show-toplevel)"
uv run --no-sync --project "$REPO/apps/pc-keiba-viewer" python - "$ROOT" <<'PY'
import json
import sys
from pathlib import Path

import duckdb

root = Path(sys.argv[1])
source = Path('/private/tmp/horse-nar-banei-0912/causal-features-raw2026.parquet')
with duckdb.connect() as connection:
    connection.execute("SET memory_limit='4GB'")
    connection.read_parquet(str(source)).create_view('history')
    connection.execute("""CREATE TEMP TABLE duplicates AS
        SELECT regexp_replace(race_id, '^(jra|nar)-', '') AS physical_race_id, horse_id,
               count(*) AS copies,
               count(DISTINCT finish) FILTER (WHERE finish>0) AS labeled_finishes,
               count(DISTINCT clock_seconds) FILTER (WHERE clock_seconds>0) AS clocks,
               count(DISTINCT field_size) AS field_sizes,
               count(*) FILTER (WHERE starts_with(race_id,'nar-')) AS native_copies,
               count(*) FILTER (WHERE starts_with(race_id,'jra-')) AS jvd_copies,
               count(*) FILTER (WHERE category!='jra') AS priority_copies
        FROM history GROUP BY 1,2 HAVING count(*)>1""")
    summary = connection.execute("""SELECT count(*) AS source_rows,
        count(DISTINCT (regexp_replace(race_id, '^(jra|nar)-', ''),horse_id)) AS canonical_rows,
        min(race_date) AS first_date, max(race_date) AS last_date,
        count(*) FILTER (WHERE race_date>='20260912') AS same_day_or_future_rows,
        count(*) FILTER (WHERE starts_with(race_id,'jra-') AND category!='jra') AS jvd_rows_with_native_priority,
        count(*) FILTER (WHERE starts_with(race_id,'nar-') AND category='jra') AS native_rows_with_jra_priority
        FROM history""").fetchdf().to_dict(orient='records')[0]
    duplicates = connection.execute("""SELECT count(*) AS duplicate_groups,
        coalesce(sum(copies-1),0) AS removed_duplicate_rows,
        count(*) FILTER (WHERE labeled_finishes>1) AS conflicting_labeled_finish_groups,
        count(*) FILTER (WHERE clocks>1) AS conflicting_positive_clock_groups,
        count(*) FILTER (WHERE field_sizes>1) AS differing_field_size_groups,
        count(*) FILTER (WHERE native_copies>0 AND jvd_copies>0) AS cross_source_groups,
        count(*) FILTER (WHERE priority_copies>1) AS tied_native_priority_groups,
        count(*) FILTER (WHERE priority_copies>1 AND labeled_finishes>1) AS tied_priority_conflicting_label_groups
        FROM duplicates""").fetchdf().to_dict(orient='records')[0]
    examples = connection.execute('SELECT * FROM duplicates WHERE labeled_finishes>1 ORDER BY physical_race_id,horse_id LIMIT 20').fetchdf().to_dict(orient='records')
    connection.read_parquet(str(root / 'timesfm-input-v2/observations.parquet')).create_view('saved_observations')
    conflict_values = connection.execute("""SELECT d.physical_race_id, d.horse_id, h.race_id AS source_race_id,
        h.category, h.finish, h.field_size, h.clock_seconds,
        CASE WHEN h.field_size>1 AND h.finish BETWEEN 1 AND h.field_size
             THEN (h.field_size-h.finish)/(h.field_size-1) END AS expected_performance,
        o.performance AS saved_performance, h.category!='jra' AS native_priority
        FROM duplicates d JOIN history h
          ON regexp_replace(h.race_id,'^(jra|nar)-','')=d.physical_race_id AND h.horse_id=d.horse_id
        JOIN saved_observations o ON o.race_id=d.physical_race_id AND o.horse_id=d.horse_id
        WHERE d.labeled_finishes>1 ORDER BY d.physical_race_id,d.horse_id,h.category""").fetchdf().to_dict(orient='records')
    source_ranges = connection.execute("""SELECT category, CASE WHEN try_cast(venue AS INTEGER)>=30 THEN 'regional' ELSE 'jra-venue' END AS venue_group,
        count(*) AS rows,min(race_date) AS first_date,max(race_date) AS last_date
        FROM history GROUP BY 1,2 ORDER BY 1,2""").fetchdf().to_dict(orient='records')
report = {'purpose':'Local-PG export canonical observation audit; no remote serving data and no new training',
          'source':str(source),'summary':summary,'duplicates':duplicates,'conflict_examples':examples,
          'source_ranges':source_ranges,'conflicting_source_values':conflict_values,
          'saved_native_conflicts_checked':sum(row['native_priority'] for row in conflict_values),
          'saved_native_conflict_mismatches':sum(row['native_priority'] and row['expected_performance'] != row['saved_performance'] for row in conflict_values),
          'selection_contract':'temporal_observations sorts category!=jra descending then keeps first physical race/horse',
          'original_training_records_changed':False,'promotion_eligible':False}
(root/'audit-canonical-observations.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print(json.dumps(report,indent=2))
PY
