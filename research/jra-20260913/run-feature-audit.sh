#!/usr/bin/env bash
# Inspect existing feature artifacts read-only; do not copy or rewrite their data.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
bash research/jra-20260913/run-research-checks.sh full-001
export PYTHONPATH="$ROOT/apps/finish-position-predict-container/src"
bash research/chronos2/run-local.sh "$ROOT/apps/timesfm-finish-position/.venv/bin/python" - <<'PY'
import json
from pathlib import Path

import duckdb
import pyarrow.dataset as ds

from train_jra_cell_models import numeric_feature_names

ROOT = Path('research/jra-20260913')
OUTPUT = ROOT / 'feature-audit-001'
OUTPUT.mkdir(exist_ok=False)
SOURCE = Path('/Users/kkk4oru/.local/share/horse-racing-data-production/jra-cell-v1/features-2000-2026-mssd-sectional-relationship-all-history-dedup-v2')
DATASET = ds.dataset(SOURCE, format='parquet', partitioning='hive')
NAMES = numeric_feature_names(DATASET.schema)
MARKET_NAMES = [name for name in NAMES if any(token in name.lower() for token in ('odds', 'ninki', 'popularity', 'market'))]
(OUTPUT / 'schema.json').write_text(json.dumps({'source_read_only': str(SOURCE), 'schema': str(DATASET.schema), 'numeric_feature_names': NAMES, 'market_related_numeric_names': MARKET_NAMES}, indent=2), encoding='utf-8')
print('FEATURE_SCHEMA', len(NAMES), 'market-related', MARKET_NAMES, flush=True)
with duckdb.connect() as connection:
    connection.execute('SET threads=4')
    connection.execute("SET memory_limit='2GB'")
    connection.execute("SET temp_directory='research/chronos2/runtime/scratch/duckdb-feature-audit'")
    connection.read_parquet([str(path) for path in SOURCE.rglob('*.parquet')], hive_partitioning=True).create_view('features')
    years = connection.execute('SELECT substr(cast(race_date as varchar),1,4) AS year, count(*) AS rows, count(distinct race_id) AS races, count(*) FILTER (WHERE finish_position IS NULL OR finish_position<=0) AS undefined FROM features GROUP BY 1 ORDER BY 1').fetchall()
    recent = connection.execute("SELECT race_id, count(*), count(*) FILTER (WHERE finish_position IS NULL OR finish_position<=0) FROM features WHERE cast(race_date as varchar)>='20260912' GROUP BY race_id ORDER BY race_id").fetchall()
    plan = json.loads((ROOT / 'source-snapshot-001/plan-20260913.json').read_text(encoding='utf-8'))
    cells = []
    for cell in plan['cells']:
        coverage = connection.execute('SELECT count(*), count(distinct race_id), min(race_date), max(race_date) FROM features WHERE race_id IN (SELECT unnest(?))',[cell['training_race_ids']]).fetchone()
        target = connection.execute('SELECT count(*), count(distinct race_id) FROM features WHERE race_id IN (SELECT unnest(?))',[cell['target_race_ids']]).fetchone()
        cells.append({'cell_id':cell['cell_id'], 'canonical':cell['canonical'], 'declared_training_races':cell['training_race_count'], 'covered_training':coverage, 'target_feature_rows_and_races':target})
    result = {'source_read_only':str(SOURCE), 'years':years, 'recent_race_rows_and_undefined':recent, 'cells':cells, 'market_related_numeric_names':MARKET_NAMES, 'production_eligible':False}
    (OUTPUT / 'coverage.json').write_text(json.dumps(result, indent=2, default=str), encoding='utf-8')
print('FEATURE_AUDIT_COMPLETE', len(cells), 'cells', flush=True)
PY
