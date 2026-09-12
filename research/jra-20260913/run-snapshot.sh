#!/usr/bin/env bash
# Read-only source inventory; preserve every output and stop on incomplete work.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
set -a
# Local, untracked runtime configuration is not part of this script's source tree.
# shellcheck source=/dev/null
source "$ROOT/apps/local-postgresql/.env"
set +a
export PGHOST=127.0.0.1
export PGPORT="${POSTGRES_PORT:?}"
export PGUSER="${POSTGRES_USER:?}"
export PGPASSWORD="${POSTGRES_PASSWORD:?}"
export PGDATABASE="${POSTGRES_DB:?}"
export PGCONNECT_TIMEOUT=10
export PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=90000'
export PYTHONPATH="$ROOT/apps/finish-position-predict-container/src"
export OMP_NUM_THREADS=4
export OPENBLAS_NUM_THREADS=4
export MKL_NUM_THREADS=4
export POLARS_MAX_THREADS=4
bash "$ROOT/research/chronos2/run-local.sh" "$ROOT/apps/timesfm-finish-position/.venv/bin/python" - <<'PY'
import hashlib
import json
from datetime import date, datetime
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from build_jra_cell_manifest import build_production_plan, load_races
from predict_lib.jra_cell_scope import cell_for_race

OUTPUT = Path('research/jra-20260913/source-snapshot-001')
OUTPUT.mkdir(exist_ok=False)
RACES = load_races('', '20000101', '20260913')
print('SOURCE_RACES_LOADED', len(RACES), flush=True)
for day in (date(2026, 9, 12), date(2026, 9, 13)):
    plan = build_production_plan(RACES, day)
    (OUTPUT / f'plan-{day:%Y%m%d}.json').write_text(
        json.dumps(plan, ensure_ascii=False, indent=2) + '\n', encoding='utf-8'
    )
    print('PLAN_SAVED', day.isoformat(), flush=True)
with psycopg.connect('', row_factory=dict_row) as connection:
    with connection.cursor() as cursor:
        cursor.execute('SHOW transaction_read_only')
        if cursor.fetchone() != {'transaction_read_only': 'on'}:
            raise ValueError('Read-only transaction is required')
        cursor.execute('''
            SELECT kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
                   umaban, ketto_toroku_bango, bamei, data_kubun,
                   ijo_kubun_code, kakutei_chakujun, tansho_odds, tansho_ninkijun
              FROM jvd_se
             WHERE kaisai_nen = '2026' AND kaisai_tsukihi IN ('0912', '0913')
               AND keibajo_code BETWEEN '01' AND '10'
             ORDER BY kaisai_tsukihi, keibajo_code, race_bango, umaban
        ''')
        rows = cursor.fetchall()
(OUTPUT / 'raw-day-starters.json').write_text(
    json.dumps(rows, ensure_ascii=False, indent=2) + '\n', encoding='utf-8'
)
SPECIAL_IDS = ('jra:2026:0913:06:11', 'jra:2026:0912:09:11')
SPECIAL = [
    {'race_id': race.race_id, 'race_name': race.race_name,
     'cell_id': cell_for_race(race).cell_id, 'canonical': cell_for_race(race).canonical,
     'runners': len(race.horse_ids)}
    for race in RACES if race.race_id in SPECIAL_IDS
]
if len(SPECIAL) != len(SPECIAL_IDS):
    raise ValueError('A required special race is absent')
RECEIPT = {
    'generated_at': datetime.now().astimezone().isoformat(),
    'source': 'read-only local PostgreSQL jvd_ra/jvd_se',
    'source_races': len(RACES), 'daily_raw_rows': len(rows),
    'special_cells': SPECIAL,
    'limitations': ['Retrospective source is not PIT attestation',
                    'NVD cross-source completeness and feature coverage not yet audited'],
    'production_eligible': False,
    'artifacts': [
        {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
        for path in sorted(OUTPUT.glob('*.json'))
    ],
}
(OUTPUT / 'receipt.json').write_text(
    json.dumps(RECEIPT, ensure_ascii=False, indent=2) + '\n', encoding='utf-8'
)
print('SNAPSHOT_COMPLETE', json.dumps(SPECIAL, ensure_ascii=False), flush=True)
PY
