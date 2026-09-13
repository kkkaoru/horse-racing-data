#!/usr/bin/env bash
# Preserve all source rivals for missing NVD seed races and inventory feature coverage.
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
import pyarrow.dataset as ds
import pyarrow.parquet as pq

root=Path('research/jra-20260913')
out=root/'nvd-full-roster-001'
out.mkdir(exist_ok=False)
census=json.loads((root/'nvd-seed-coverage-001/report.json').read_text(encoding='utf-8'))
ids=sorted({rid for case in census['results'] for rid in case['new_race_ids']})
parts=[rid.split(':') for rid in ids]
parameters=tuple([p[index] for p in parts] for index in (1,2,3,4))
selection='JOIN unnest(%s::text[],%s::text[],%s::text[],%s::text[]) AS wanted(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango) USING(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)'
with psycopg.connect('') as connection:
    with connection.cursor() as cursor:
        cursor.execute('SELECT to_jsonb(se) FROM nvd_se se '+selection,parameters)
        runners=[row[0] for row in cursor.fetchall()]
        cursor.execute('SELECT to_jsonb(ra) FROM nvd_ra ra '+selection,parameters)
        races=[row[0] for row in cursor.fetchall()]
for name,records in [('runners',runners),('races',races)]:
    with gzip.open(out/f'{name}.json.gz','wt',encoding='utf-8') as stream:
        json.dump(records,stream,ensure_ascii=False)
se=pd.DataFrame(runners)
ra=pd.DataFrame(races)
for frame in (se,ra):
    frame['race_id']='nar:'+frame['kaisai_nen']+':'+frame['kaisai_tsukihi']+':'+frame['keibajo_code']+':'+frame['race_bango']
active=se.loc[~se['ijo_kubun_code'].str.strip().isin(['1','2','3'])]
counts=active.groupby('race_id').size().rename('active_rows')
ra['declared_size']=pd.to_numeric(ra['shusso_tosu'].str.strip(),errors='coerce')
comparison=ra[['race_id','declared_size']].merge(counts,on='race_id',how='left',validate='one_to_one')
comparison['equal']=comparison['declared_size'].eq(comparison['active_rows']) & comparison['declared_size'].gt(0)
comparison.to_json(out/'field-counts.json',orient='records',indent=2)
print('NVD_FULL_ROSTERS',len(ids),'requested',len(ra),'RA',len(se),'SE',int(comparison['equal'].sum()),'matching fields',flush=True)
feature_source=Path('apps/pc-keiba-viewer/tmp/feat-nar-v9-weather')
dataset=ds.dataset(feature_source,format='parquet',partitioning='hive')
features=json.loads((root/'priority-capacity-001/marketfree/year-2023/protocol.json').read_text(encoding='utf-8'))['feature_names']
missing=[name for name in features if name not in dataset.schema.names]
columns=['race_id','ketto_toroku_bango','umaban',*[name for name in features if name in dataset.schema.names]]
selected=dataset.to_table(columns=columns,filter=ds.field('race_id').isin(ids))
pq.write_table(selected,out/'available-feature-rows.parquet',compression='zstd')
feature_frame=selected.to_pandas()
feature_keys=feature_frame[['race_id','ketto_toroku_bango','umaban']].copy()
feature_keys['umaban']=pd.to_numeric(feature_keys['umaban'],errors='raise')
active_keys=active[['race_id','ketto_toroku_bango','umaban']].copy()
active_keys['umaban']=pd.to_numeric(active_keys['umaban'],errors='raise')
matched=active_keys.merge(feature_keys,on=['race_id','ketto_toroku_bango','umaban'],how='left',indicator=True,validate='one_to_one')
report={'requested_races':len(ids),'source_race_rows':len(ra),'source_runner_rows':len(se),'active_rows':len(active),'count_matching_races':int(comparison['equal'].sum()),'count_mismatches':comparison.loc[~comparison['equal']].to_dict(orient='records'),'feature_source':str(feature_source),'available_feature_rows':selected.num_rows,'active_rows_with_feature_identity':int(matched['_merge'].eq('both').sum()),'missing_required_feature_columns':missing,'source_files':{name:hashlib.sha256((out/name).read_bytes()).hexdigest() for name in ['runners.json.gz','races.json.gz','available-feature-rows.parquet']},'original_statuses_preserved':True,'training_population_not_changed':True,'production_eligible':False}
(out/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print('NVD_FEATURE_COVERAGE',report['active_rows_with_feature_identity'],'/',len(active),'missing schema columns',len(missing),flush=True)
PY
