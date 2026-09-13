#!/usr/bin/env bash
# Preserve the canonical SE-to-RA history join before reconstructing margins.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
set -a
# Local credentials are runtime configuration, not a checked-in shell module.
# shellcheck source=/dev/null
source apps/local-postgresql/.env
set +a
export PGHOST=127.0.0.1 PGPORT="${POSTGRES_PORT:?}" PGUSER="${POSTGRES_USER:?}"
export PGPASSWORD="${POSTGRES_PASSWORD:?}" PGDATABASE="${POSTGRES_DB:?}" PGCONNECT_TIMEOUT=10
export PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=90000'
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import gzip
import hashlib
import json
from pathlib import Path

import pandas as pd
import psycopg

root=Path('research/jra-20260913')
out=root/'nvd-history-race-metadata-001'
out.mkdir(exist_ok=False)
f=pd.read_parquet(root/'common-schema-source-001/nvd-teachers.parquet',columns=['horse_id'])
horses=sorted(f['horse_id'].unique().tolist())
query="""
SELECT DISTINCT ra.kaisai_nen,ra.kaisai_tsukihi,ra.keibajo_code,ra.race_bango,
 ra.data_kubun,ra.shusso_tosu,ra.track_code,ra.kyori,ra.grade_code,ra.kyoso_joken_code,ra.zenhan_3f
FROM nvd_ra ra JOIN nvd_se se USING(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)
WHERE se.ketto_toroku_bango=ANY(%s)
 AND ra.kaisai_nen||ra.kaisai_tsukihi BETWEEN '20000101' AND '20221231'
"""
with psycopg.connect('') as connection, connection.cursor() as cursor:
    cursor.execute(query,(horses,))
    if cursor.description is None:
        raise ValueError('Missing race metadata schema')
    columns=[field.name for field in cursor.description]
    records=[dict(zip(columns,row,strict=True)) for row in cursor.fetchall()]
metadata=pd.DataFrame(records)
if metadata.duplicated(['kaisai_nen','kaisai_tsukihi','keibajo_code','race_bango']).any():
    raise ValueError('Race metadata revisions require independent audit')
path=out/'races.json.gz'
with gzip.open(path,'wt',encoding='utf-8') as stream:
    json.dump(records,stream,ensure_ascii=False)
with path.open('rb') as stream:
    digest=hashlib.file_digest(stream,'sha256').hexdigest()
report={'known_rival_horses':len(horses),'race_rows':len(records),'sha256':digest,'scope':['20000101','20221231'],'purpose':'Match canonical raw history INNER JOIN to RA, not an independently certified full roster','production_eligible':False}
(out/'query.sql').write_text(query,encoding='utf-8')
(out/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print('NVD_HISTORY_RA_COMPLETE',report,flush=True)
PY
