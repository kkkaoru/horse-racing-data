#!/usr/bin/env bash
# Quantify missing NVD seed-horse histories without changing training populations.
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
import json
from pathlib import Path

import pandas as pd
import psycopg

root=Path('research/jra-20260913')
out=root/'nvd-seed-coverage-001'
out.mkdir(exist_ok=False)
cases=[]
for year in (2020,2021,2022,2023):
    cases.extend(json.loads((root/f'priority-capacity-001/marketfree/year-{year}/cohorts.json').read_text(encoding='utf-8')))
seeds=sorted({horse for case in cases for horse in case['seed_horse_ids']})
if '0000000000' in seeds:
    raise ValueError('Anonymous identities cannot seed cross-source history queries')
with psycopg.connect('') as connection:
    with connection.cursor() as cursor:
        cursor.execute("""SELECT 'nar:'||kaisai_nen||':'||kaisai_tsukihi||':'||keibajo_code||':'||race_bango AS race_id, kaisai_nen||kaisai_tsukihi AS race_date, trim(ketto_toroku_bango), trim(umaban), trim(ijo_kubun_code), trim(data_kubun) FROM nvd_se WHERE ketto_toroku_bango=ANY(%s) AND kaisai_nen||kaisai_tsukihi BETWEEN '20000101' AND '20221231' ORDER BY kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,ketto_toroku_bango""",(seeds,))
        frame=pd.DataFrame(cursor.fetchall(),columns=['race_id','race_date','horse_id','horse_number','abnormality_code','source_status'])
frame.to_parquet(out/'nvd-seed-history.parquet',index=False)
results=[]
for case in cases:
    subset=frame.loc[frame['horse_id'].isin(case['seed_horse_ids']) & frame['race_date'].ge(case['history_start'].replace('-','')) & frame['race_date'].lt(case['cutoff'].replace('-',''))].copy()
    subset['jvd_equivalent_key']=subset['race_id'].str.replace('nar:','jra:',regex=False)
    absent=subset.loc[~subset['jvd_equivalent_key'].isin(case['training_race_ids'])]
    result={'year':case['evaluation_year'],'cell':case['cell_id'],'seed_horses':len(case['seed_horse_ids']),'nvd_history_rows':len(subset),'nvd_history_races':int(subset['race_id'].nunique()),'nvd_horses_with_history':int(subset['horse_id'].nunique()),'rows_outside_jvd_training_race_keys':len(absent),'races_outside_jvd_training_race_keys':int(absent['race_id'].nunique()),'new_race_ids':sorted(absent['race_id'].unique().tolist())}
    results.append(result)
    print('NVD_HISTORY_GAP',result['year'],result['cell'],result['nvd_history_rows'],result['races_outside_jvd_training_race_keys'],flush=True)
(out/'report.json').write_text(json.dumps({'purpose':'source-scope census only, not a new training population','seed_count':len(seeds),'raw_rows':len(frame),'raw_statuses_retained':True,'full_rival_rosters_not_yet_fetched':True,'production_eligible':False,'results':results},indent=2),encoding='utf-8')
PY
