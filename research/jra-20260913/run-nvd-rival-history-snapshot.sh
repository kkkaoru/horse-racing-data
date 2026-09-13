#!/usr/bin/env bash
# Collect historical inputs for the complete new rival population, not just seeds.
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
out=root/'nvd-rival-history-001'
out.mkdir(exist_ok=False)
with gzip.open(root/'nvd-full-roster-001/runners.json.gz','rt',encoding='utf-8') as stream:
    se=pd.DataFrame(json.load(stream))
with gzip.open(root/'nvd-full-roster-001/races.json.gz','rt',encoding='utf-8') as stream:
    ra=pd.DataFrame(json.load(stream))
keys=['kaisai_nen','kaisai_tsukihi','keibajo_code','race_bango']
ra['race_id']='nar:'+ra['kaisai_nen']+':'+ra['kaisai_tsukihi']+':'+ra['keibajo_code']+':'+ra['race_bango']
cancelled=ra.loc[ra['data_kubun'].str.strip().isin(['0','9'])]
(cancelled[['race_id','data_kubun','shusso_tosu']]).to_json(out/'noneligible-race-ledger.json',orient='records',indent=2)
normal_keys=ra.loc[~ra['data_kubun'].str.strip().isin(['0','9']),keys]
normal=se.merge(normal_keys,on=keys,how='inner',validate='many_to_one')
active=normal.loc[~normal['ijo_kubun_code'].str.strip().isin(['1','2','3'])]
unknown=active['ketto_toroku_bango'].isna() | active['ketto_toroku_bango'].str.strip().isin(['','0000000000'])
horses=sorted(active.loc[~unknown,'ketto_toroku_bango'].str.strip().unique().tolist())
with psycopg.connect('') as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_jsonb(rec) FROM race_entry_corner_features rec WHERE ketto_toroku_bango=ANY(%s) AND race_date BETWEEN '20000101' AND '20221231'",(horses,))
        records=[row[0] for row in cursor.fetchall()]
        print('CORNER_HISTORY_FETCHED',len(records),flush=True)
        cursor.execute("SELECT to_jsonb(se) FROM nvd_se se WHERE ketto_toroku_bango=ANY(%s) AND kaisai_nen||kaisai_tsukihi BETWEEN '20000101' AND '20221231'",(horses,))
        weights=[row[0] for row in cursor.fetchall()]
files=[]
for name,rows in [('corner-history',records),('nvd-runner-history',weights)]:
    path=out/f'{name}.json.gz'
    with gzip.open(path,'wt',encoding='utf-8') as stream:
        json.dump(rows,stream,ensure_ascii=False)
    files.append({'path':str(path),'rows':len(rows),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
report={'normal_races':len(normal_keys),'active_target_rows':len(active),'known_rival_horses':len(horses),'unknown_target_identity_rows':int(unknown.sum()),'noneligible_races':len(cancelled),'history_window':['20000101','20221231'],'temporal_warning':'each target must use only strictly prior histories; this collection is not a feature matrix','source_warning':'NVD body weights are fetched directly; legacy sectional stage hardcodes pg.jvd_se and must not silently drop NVD weights','files':files,'production_eligible':False}
(out/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print('NVD_RIVAL_HISTORY_COMPLETE',report,flush=True)
PY
