#!/usr/bin/env bash
# Preserve label/eligibility source for matched, complete-roster model experiments.
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
bash research/chronos2/run-local.sh "$ROOT/apps/timesfm-finish-position/.venv/bin/python" - <<'PY'
import hashlib
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

OUT = Path('research/jra-20260913/training-status-001')
OUT.mkdir(exist_ok=False)
with psycopg.connect('') as connection:
    with connection.cursor() as cursor:
        cursor.execute('''
            SELECT 'jra:' || se.kaisai_nen || ':' || se.kaisai_tsukihi || ':' ||
                   se.keibajo_code || ':' || se.race_bango AS race_id,
                   trim(se.ketto_toroku_bango) AS horse_id,
                   trim(se.ijo_kubun_code) AS abnormality_code,
                   trim(se.kakutei_chakujun) AS finish_text,
                   trim(se.data_kubun) AS source_status
              FROM jvd_se se JOIN jvd_ra ra
                USING (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
             WHERE se.kaisai_nen || se.kaisai_tsukihi BETWEEN '20000101' AND '20260912'
               AND coalesce(trim(ra.data_kubun), '') NOT IN ('0','9')
             ORDER BY se.kaisai_nen, se.kaisai_tsukihi, se.keibajo_code, se.race_bango,
                      se.ketto_toroku_bango
        ''')
        rows = cursor.fetchall()
FRAME = pd.DataFrame(rows, columns=['race_id','horse_id','abnormality_code','finish_text','source_status'])
if FRAME.duplicated(['race_id','horse_id']).any():
    raise ValueError('Duplicate source runner identities require a separate revision audit')
PATH = OUT / 'jvd-runner-status.parquet'
pq.write_table(pa.Table.from_pandas(FRAME, preserve_index=False), PATH, compression='zstd')
with PATH.open('rb') as stream:
    digest = hashlib.file_digest(stream, 'sha256').hexdigest()
(OUT / 'receipt.json').write_text(json.dumps({'generated_at':datetime.now().astimezone().isoformat(),'rows':len(FRAME),'races':int(FRAME['race_id'].nunique()),'path':str(PATH),'sha256':digest,'source':'read-only PostgreSQL eligibility/historical label snapshot; Cloudflare remains authority for 9/12 actual results','training_cutoff_inclusive':'20260912','production_eligible':False}, indent=2), encoding='utf-8')
print('TRAINING_STATUS_SNAPSHOT_COMPLETE', len(FRAME), 'rows', digest, flush=True)
PY
