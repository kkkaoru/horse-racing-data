#!/usr/bin/env bash
# Resolve undefined feature labels against source status without inventing ranks.
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
bash research/chronos2/run-local.sh "$ROOT/apps/timesfm-finish-position/.venv/bin/python" - <<'PY'
import json
from collections import Counter
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from build_jra_cell_manifest import load_races
from predict_lib.jra_cell_scope import cell_for_race

ROOT = Path('research/jra-20260913')
OUT = ROOT / 'label-status-001'
OUT.mkdir(exist_ok=False)
AUDIT = json.loads((ROOT / 'fold-scopes-001/undefined-feature-labels.json').read_text(encoding='utf-8'))
KEYS = [dict(zip(('y','md','v','r','h'), (*row[0].split(':')[1:], row[1]), strict=True)) for row in AUDIT['active_roster_undefined']]
with psycopg.connect('', row_factory=dict_row) as connection:
    with connection.cursor() as cursor:
        cursor.execute('''
            WITH keys AS (
                SELECT * FROM jsonb_to_recordset(%s::jsonb)
                AS k(y text, md text, v text, r text, h text)
            )
            SELECT k.*, se.ijo_kubun_code, se.kakutei_chakujun, se.data_kubun
            FROM keys k LEFT JOIN jvd_se se
              ON se.kaisai_nen=k.y AND se.kaisai_tsukihi=k.md
             AND se.keibajo_code=k.v AND se.race_bango=k.r
             AND se.ketto_toroku_bango=k.h
        ''', (json.dumps(KEYS),))
        rows = cursor.fetchall()
if len(rows) != len(KEYS):
    raise ValueError('Source label join cardinality changed')
(OUT / 'rows.json').write_text(json.dumps(rows, indent=2), encoding='utf-8')
SUMMARY = Counter((row['ijo_kubun_code'], row['data_kubun'], row['kakutei_chakujun']) for row in rows)
(OUT / 'summary.json').write_text(json.dumps({'counts':[{'ijo':key[0],'source_status':key[1],'finish':key[2],'rows':count} for key,count in SUMMARY.items()], 'source_read_only':True}, indent=2), encoding='utf-8')
print('LABEL_STATUS_COUNTS', dict(SUMMARY), flush=True)
RACES = load_races('', '20000101', '20260913')
NAMED = [race for race in RACES if race.venue in ('06','09') and any(name in race.race_name for name in ('セントライト','チャレンジカップ'))]
HISTORY = [{'race_id':race.race_id,'date':race.race_date.isoformat(),'name':race.race_name.strip(),'canonical':cell_for_race(race).canonical,'runners':len(race.horse_ids)} for race in NAMED]
(OUT / 'named-event-history.json').write_text(json.dumps(HISTORY, ensure_ascii=False, indent=2), encoding='utf-8')
print('NAMED_EVENT_HISTORY', len(HISTORY), flush=True)
print('LABEL_STATUS_AUDIT_COMPLETE', flush=True)
PY
