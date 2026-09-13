#!/usr/bin/env bash
# Check shared feature semantics, not merely column-name overlap.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import gzip
import hashlib
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

root=Path('research/jra-20260913')
out=root/'nvd-margin-contract-001'
out.mkdir(exist_ok=False)
target=pd.read_parquet(root/'common-schema-source-001/nvd-teachers.parquet',columns=['race_id','horse_id','horse_number','speed_index_avg_5','speed_index_best_5'])
with gzip.open(root/'nvd-rival-history-001/nvd-runner-history.json.gz','rt',encoding='utf-8') as stream:
    records=json.load(stream)
columns=['kaisai_nen','kaisai_tsukihi','keibajo_code','race_bango','ketto_toroku_bango','umaban','time_sa','kakutei_chakujun','ijo_kubun_code']
raw=pd.DataFrame.from_records(records,columns=columns)
del records
with gzip.open(root/'nvd-rival-history-001/corner-history.json.gz','rt',encoding='utf-8') as stream:
    records=json.load(stream)
view=pd.DataFrame.from_records(records,columns=['source','kaisai_nen','kaisai_tsukihi','keibajo_code','race_bango','ketto_toroku_bango','umaban','time_sa'])
del records
view=view.loc[view['source'].eq('nar')].copy()
view['umaban']=pd.to_numeric(view['umaban'],errors='raise')
identity=['kaisai_nen','kaisai_tsukihi','keibajo_code','race_bango','ketto_toroku_bango','umaban']
raw['umaban']=pd.to_numeric(raw['umaban'],errors='raise')
if raw.duplicated(identity).any() or view.duplicated(identity).any():
    raise ValueError('Historical identities need a revision audit before semantic comparison')
with duckdb.connect() as con:
    con.execute('SET threads=4')
    con.execute("SET memory_limit='2GB'")
    con.execute(f"SET temp_directory='{out.as_posix()}/scratch'")
    con.register('targets',target)
    con.register('raw_history',raw)
    con.register('view_history',view)
    sql="""
    WITH h AS (
      SELECT r.*, r.kaisai_nen||r.kaisai_tsukihi AS race_date,
             strptime(r.kaisai_nen||r.kaisai_tsukihi,'%Y%m%d') AS race_dt,
             try_cast(nullif(trim(r.time_sa),'') AS DOUBLE) AS raw_margin,
             try_cast(nullif(trim(r.time_sa),'0000') AS DOUBLE)/10 AS seconds_zero_missing,
             v.time_sa AS materialized_margin
      FROM raw_history r LEFT JOIN view_history v USING(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,ketto_toroku_bango,umaban)
      WHERE try_cast(r.kakutei_chakujun AS INTEGER)>0 AND trim(r.ijo_kubun_code) NOT IN ('1','2','3')
    ), ranked AS (
      SELECT t.race_id,t.horse_id,t.horse_number,h.raw_margin,h.seconds_zero_missing,h.materialized_margin,
             row_number() OVER(PARTITION BY t.race_id,t.horse_id,t.horse_number ORDER BY h.race_date DESC,h.keibajo_code,h.race_bango) AS recent_rank
      FROM targets t JOIN h ON h.ketto_toroku_bango=t.horse_id
       AND h.race_date<substr(t.race_id,5,4)||substr(t.race_id,10,4)
       AND h.race_dt>=strptime(substr(t.race_id,5,4)||substr(t.race_id,10,4),'%Y%m%d')-INTERVAL '10 years'
    )
    SELECT race_id,horse_id,horse_number,
           avg(raw_margin) AS mean_raw,min(raw_margin) AS best_raw,
           avg(raw_margin)/10 AS mean_seconds,min(raw_margin)/10 AS best_seconds,
           avg(seconds_zero_missing) AS mean_seconds_zero_missing,min(seconds_zero_missing) AS best_seconds_zero_missing,
           avg(materialized_margin) AS mean_materialized,min(materialized_margin) AS best_materialized,
           count(*) AS prior_rows_used
      FROM ranked WHERE recent_rank<=5 GROUP BY race_id,horse_id,horse_number
    """
    (out/'query.sql').write_text(sql,encoding='utf-8')
    derived=con.execute(sql).fetchdf()
joined=target.merge(derived,on=['race_id','horse_id','horse_number'],how='left',validate='one_to_one')
joined.to_parquet(out/'comparison.parquet',index=False)
results=[]
for feature,prefix in [('speed_index_avg_5','mean'),('speed_index_best_5','best')]:
    expected=np.asarray(joined[feature],dtype=np.float64)
    for variant in ('raw','seconds','seconds_zero_missing','materialized'):
        candidate=np.asarray(joined[f'{prefix}_{variant}'],dtype=np.float64)
        available=np.isfinite(expected)&np.isfinite(candidate)
        agreement=np.isclose(expected,candidate,atol=1e-6,rtol=1e-6)
        results.append({'feature':feature,'variant':variant,'comparable_rows':int(available.sum()),'equal_rows':int((available&agreement).sum()),'median_absolute_error':float(np.median(np.abs(expected[available]-candidate[available]))) if available.any() else None})
source=Path('apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py')
report={'purpose':'numeric contract diagnostic only; no rescaling or teacher mutation','canonical_definition':'horse_career_cte averages/minimizes time_sa; current raw source reader divides time_sa by10 and treats0000 as missing','canonical_source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'rows':len(joined),'results':results,'production_eligible':False}
(out/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print('NVD_MARGIN_CONTRACT',results,flush=True)
PY
