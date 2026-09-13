#!/usr/bin/env bash
# Verify the historical JRA target scale before supplying restored NAR margins.
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

import duckdb
import numpy as np
import pandas as pd
import psycopg
import pyarrow.dataset as ds

root=Path('research/jra-20260913')
out=root/'jra-margin-contract-002'
out.mkdir(exist_ok=False)
races=[]
for year in (2020,2021,2022,2023):
    cohorts=json.loads((root/f'priority-capacity-001/marketfree/year-{year}/cohorts.json').read_text(encoding='utf-8'))
    for cohort in cohorts:
        races.extend(cohort['evaluation_race_ids'])
source=Path('/Users/kkk4oru/.local/share/horse-racing-data-production/jra-cell-v1/features-2000-2026-mssd-sectional-relationship-all-history-dedup-v2')
target=ds.dataset(source,format='parquet',partitioning='hive').to_table(columns=['race_id','ketto_toroku_bango','umaban','speed_index_avg_5','speed_index_best_5'],filter=ds.field('race_id').isin(races)).to_pandas()
if target.empty or set(target['race_id'])!=set(races):
    raise ValueError('All requested development events must be present')
horses=sorted(target['ketto_toroku_bango'].unique().tolist())
query="""SELECT jsonb_build_object('kaisai_nen',kaisai_nen,'kaisai_tsukihi',kaisai_tsukihi,'keibajo_code',keibajo_code,'race_bango',race_bango,'ketto_toroku_bango',ketto_toroku_bango,'umaban',umaban,'time_sa',time_sa,'kakutei_chakujun',kakutei_chakujun,'ijo_kubun_code',ijo_kubun_code,'has_ra',EXISTS(SELECT 1 FROM jvd_ra ra WHERE ra.kaisai_nen=se.kaisai_nen AND ra.kaisai_tsukihi=se.kaisai_tsukihi AND ra.keibajo_code=se.keibajo_code AND ra.race_bango=se.race_bango)) FROM jvd_se se WHERE ketto_toroku_bango=ANY(%s) AND kaisai_nen||kaisai_tsukihi BETWEEN '20000101' AND '20231231'"""
with psycopg.connect('') as connection, connection.cursor() as cursor:
    cursor.execute(query,(horses,))
    records=[row[0] for row in cursor.fetchall()]
with gzip.open(out/'raw-histories.json.gz','wt',encoding='utf-8') as stream:
    json.dump(records,stream,ensure_ascii=False)
history=pd.DataFrame(records)
identity=['kaisai_nen','kaisai_tsukihi','keibajo_code','race_bango','ketto_toroku_bango','umaban']
if history.duplicated(identity).any():
    raise ValueError('Historical JRA runner revisions require investigation')
with duckdb.connect() as con:
    con.execute('SET threads=4')
    con.execute("SET memory_limit='1GB'")
    con.execute(f"SET temp_directory='{out.as_posix()}/scratch'")
    con.register('targets',target)
    con.register('history',history)
    sql="""
    WITH h AS (
      SELECT *,kaisai_nen||kaisai_tsukihi AS race_date,
       strptime(kaisai_nen||kaisai_tsukihi,'%Y%m%d') AS race_dt,
       try_cast(nullif(trim(time_sa),'') AS DOUBLE) AS raw_margin,
       try_cast(nullif(nullif(trim(time_sa),''),'0000') AS DOUBLE)/10 AS canonical_seconds
      FROM history WHERE has_ra AND try_cast(kakutei_chakujun AS INTEGER)>0
    ), ranked AS (
      SELECT t.race_id,t.ketto_toroku_bango,t.umaban,h.raw_margin,h.canonical_seconds,
       row_number() OVER(PARTITION BY t.race_id,t.ketto_toroku_bango,t.umaban ORDER BY h.race_date DESC,h.keibajo_code,h.race_bango) AS rn
      FROM targets t JOIN h ON t.ketto_toroku_bango=h.ketto_toroku_bango
       AND h.race_date<substr(t.race_id,5,4)||substr(t.race_id,10,4)
       AND h.race_dt>=strptime(substr(t.race_id,5,4)||substr(t.race_id,10,4),'%Y%m%d')-INTERVAL '10 years'
    ) SELECT race_id,ketto_toroku_bango,umaban,
       avg(raw_margin) AS mean_raw,min(raw_margin) AS best_raw,
       avg(raw_margin)/10 AS mean_seconds,min(raw_margin)/10 AS best_seconds,
       avg(canonical_seconds) AS mean_canonical,min(canonical_seconds) AS best_canonical
      FROM ranked WHERE rn<=5 GROUP BY race_id,ketto_toroku_bango,umaban
    """
    derived=con.execute(sql).fetchdf()
joined=target.merge(derived,on=['race_id','ketto_toroku_bango','umaban'],how='left',validate='one_to_one')
joined.to_parquet(out/'comparison.parquet',index=False)
results=[]
for feature,prefix in [('speed_index_avg_5','mean'),('speed_index_best_5','best')]:
    expected=np.asarray(joined[feature],dtype=np.float64)
    for variant in ('raw','seconds','canonical'):
        value=np.asarray(joined[f'{prefix}_{variant}'],dtype=np.float64)
        comparable=np.isfinite(expected)&np.isfinite(value)
        results.append({'feature':feature,'variant':variant,'stored_nonmissing':int(np.isfinite(expected).sum()),'comparable_rows':int(comparable.sum()),'equal_rows':int((comparable&np.isclose(expected,value,rtol=1e-6,atol=1e-6)).sum())})
with (out/'raw-histories.json.gz').open('rb') as stream:
    digest=hashlib.file_digest(stream,'sha256').hexdigest()
report={'scope':'2020-2023 development event inputs only; no20260912 numeric data','races':races,'rows':len(target),'history_rows':len(history),'history_sha256':digest,'results':results,'production_eligible':False}
(out/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
(out/'query.sql').write_text(sql,encoding='utf-8')
print('JRA_MARGIN_CONTRACT',report,flush=True)
PY
