#!/usr/bin/env bash
# Freeze exact-cell evaluation scopes and broadened historical labels before fitting.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
set -a
# shellcheck source=/dev/null
source apps/local-postgresql/.env
set +a
export PGHOST=127.0.0.1 PGPORT="${POSTGRES_PORT:?}" PGUSER="${POSTGRES_USER:?}"
export PGPASSWORD="${POSTGRES_PASSWORD:?}" PGDATABASE="${POSTGRES_DB:?}" PGCONNECT_TIMEOUT=10
export PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=90000'
export PYTHONPATH="$ROOT/apps/finish-position-predict-container/src"
export OMP_NUM_THREADS=4 OPENBLAS_NUM_THREADS=4 MKL_NUM_THREADS=4
bash research/chronos2/run-local.sh "$ROOT/apps/timesfm-finish-position/.venv/bin/python" - <<'PY'
import json
from collections import Counter
from dataclasses import asdict
from datetime import date
from pathlib import Path

import duckdb

from build_jra_cell_manifest import load_races
from predict_lib.jra_cell_scope import JraRaceIndex, cell_for_race

ROOT = Path('research/jra-20260913')
OUTPUT = ROOT / 'fold-scopes-001'
OUTPUT.mkdir(exist_ok=False)
SOURCE = Path('/Users/kkk4oru/.local/share/horse-racing-data-production/jra-cell-v1/features-2000-2026-mssd-sectional-relationship-all-history-dedup-v2')
RACES = load_races('', '20000101', '20260913')
BY_ID = {race.race_id:race for race in RACES}
if len(BY_ID) != len(RACES):
    raise ValueError('Duplicate physical race identities')
INDEX = JraRaceIndex([race for race in RACES if race.race_date < date(2026,9,13)])
PLAN = json.loads((ROOT / 'source-snapshot-001/plan-20260913.json').read_text(encoding='utf-8'))
TARGETS = {cell['cell_id']:cell_for_race(BY_ID[cell['target_race_ids'][0]]) for cell in PLAN['cells']}
SPECIAL = cell_for_race(BY_ID['jra:2026:0912:09:11'])
TARGETS[SPECIAL.cell_id] = SPECIAL
with duckdb.connect() as connection:
    connection.execute('SET threads=4')
    connection.execute("SET memory_limit='2GB'")
    connection.execute("SET temp_directory='research/chronos2/runtime/scratch/duckdb-fold-audit'")
    connection.read_parquet([str(path) for path in SOURCE.rglob('*.parquet')], hive_partitioning=True).create_view('features')
    feature_races = set(row[0] for row in connection.execute('SELECT DISTINCT race_id FROM features').fetchall())
    invalid = connection.execute('SELECT race_id, ketto_toroku_bango, finish_position FROM features WHERE finish_position IS NULL OR finish_position<=0').fetchall()
    active = {race.race_id:set(race.horse_ids) for race in RACES}
    active_invalid = [row for row in invalid if row[1] in active.get(row[0], set())]
    nonstarter_invalid = [row for row in invalid if row[1] not in active.get(row[0], set())]
    (OUTPUT / 'undefined-feature-labels.json').write_text(json.dumps({'undefined_count':len(invalid), 'active_roster_undefined':active_invalid, 'not_in_active_roster_count':len(nonstarter_invalid), 'definition':'Active roster from source revision and ijo_kubun exclusion, not positive finishing labels'}, indent=2), encoding='utf-8')
    summaries = []
    for cell_id, cell in TARGETS.items():
        folds = []
        for year in range(2020,2027):
            scope = INDEX.build_fold_scope(cell, year)
            payload = asdict(scope)
            payload['missing_training_feature_races'] = sorted(set(scope.training_race_ids) - feature_races)
            payload['missing_evaluation_feature_races'] = sorted(set(scope.evaluation_race_ids) - feature_races)
            payload['training_venue_counts'] = dict(Counter(BY_ID[race_id].venue for race_id in scope.training_race_ids))
            folds.append(payload)
        (OUTPUT / f'{cell_id}.json').write_text(json.dumps({'cell_id':cell_id,'canonical':cell.canonical,'folds':folds,'production_eligible':False}, indent=2, default=str), encoding='utf-8')
        summary = {'cell_id':cell_id,'canonical':cell.canonical,'evaluation_races_by_year':{f['evaluation_year']:len(f['evaluation_race_ids']) for f in folds},'missing_evaluation_features_by_year':{f['evaluation_year']:len(f['missing_evaluation_feature_races']) for f in folds},'training_races_by_year':{f['evaluation_year']:len(f['training_race_ids']) for f in folds}}
        summaries.append(summary)
        print('FOLD_SCOPE', summary, flush=True)
    (OUTPUT / 'summary.json').write_text(json.dumps({'cells':summaries,'undefined_active_labels':len(active_invalid),'undefined_nonstarter_or_unmapped_labels':len(nonstarter_invalid),'source_limitations':['JVD-only graph; NVD coverage pending','retrospective yearly entrant-cohort selection; not live PIT attestation'],'production_eligible':False}, indent=2), encoding='utf-8')
print('FOLD_SCOPE_AUDIT_COMPLETE', len(TARGETS), flush=True)
PY
