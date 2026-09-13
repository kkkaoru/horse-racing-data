#!/usr/bin/env bash
# Verify existing first-corner semantics before restoring absent corner2..4 columns.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import gzip
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

root=Path('research/jra-20260913')
out=root/'nvd-corner-contract-001'
out.mkdir(exist_ok=False)
keys=['kaisai_nen','kaisai_tsukihi','keibajo_code','race_bango']
identity=['race_id','horse_id','horse_number']
features=['past_corner_1_norm_avg_3','past_corner_1_norm_avg_5','past_corner_1_norm_avg_10']
target=pd.read_parquet(root/'common-schema-source-001/nvd-teachers.parquet',columns=identity+features)
with gzip.open(root/'nvd-rival-history-001/nvd-runner-history.json.gz','rt',encoding='utf-8') as stream:
    records=json.load(stream)
raw=pd.DataFrame.from_records(records,columns=[*keys,'ketto_toroku_bango','umaban','kakutei_chakujun','corner_1'])
del records
with gzip.open(root/'nvd-history-race-metadata-001/races.json.gz','rt',encoding='utf-8') as stream:
    metadata=pd.DataFrame(json.load(stream))
with duckdb.connect() as con:
    con.execute('SET threads=4')
    con.execute("SET memory_limit='2GB'")
    con.execute(f"SET temp_directory='{out.as_posix()}/scratch'")
    con.register('targets',target)
    con.register('raw_history',raw)
    con.register('metadata',metadata)
    con.execute("CREATE TEMP TABLE h AS SELECT r.*,strptime(r.kaisai_nen||r.kaisai_tsukihi,'%Y%m%d') AS race_dt,try_cast(m.shusso_tosu AS INTEGER) AS field_size,try_cast(nullif(nullif(trim(r.corner_1),''),'00') AS DOUBLE) AS corner FROM raw_history r JOIN metadata m USING(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango) WHERE try_cast(r.kakutei_chakujun AS INTEGER)>0 AND try_cast(m.kyori AS INTEGER) IS NOT NULL")
    invalid=con.execute('SELECT * FROM h WHERE field_size IS NULL OR field_size<=0').fetchdf()
    invalid.to_parquet(out/'requires-full-peer-fallback.parquet',index=False)
    if not invalid.empty:
        raise ValueError('Canonical field-size fallback requires complete prior-race peer records')
    sql="""
    WITH ranked AS (
      SELECT t.race_id,t.horse_id,t.horse_number,
       CASE WHEN h.field_size>1 AND h.corner IS NOT NULL THEN (h.corner-1)/(h.field_size-1) ELSE NULL END AS corner_norm,
       row_number() OVER(PARTITION BY t.race_id,t.horse_id,t.horse_number ORDER BY h.race_dt DESC,h.keibajo_code,h.race_bango) AS rn
      FROM targets t JOIN h ON t.horse_id=h.ketto_toroku_bango
       AND h.race_dt<strptime(substr(t.race_id,5,4)||substr(t.race_id,10,4),'%Y%m%d')
       AND h.race_dt>=strptime(substr(t.race_id,5,4)||substr(t.race_id,10,4),'%Y%m%d')-INTERVAL '10 years'
    ) SELECT race_id,horse_id,horse_number,
       avg(corner_norm) FILTER(WHERE rn<=3) AS mean3,
       avg(corner_norm) FILTER(WHERE rn<=5) AS mean5,
       avg(corner_norm) FILTER(WHERE rn<=10) AS mean10
      FROM ranked WHERE rn<=10 GROUP BY race_id,horse_id,horse_number
    """
    derived=con.execute(sql).fetchdf()
joined=target.merge(derived,on=identity,how='left',validate='one_to_one')
joined.to_parquet(out/'comparison.parquet',index=False)
complete=(joined['race_id'].str.slice(4,8)+joined['race_id'].str.slice(9,13)).ge('20100101').to_numpy()
results=[]
for window in (3,5,10):
    stored=joined[f'past_corner_1_norm_avg_{window}'].to_numpy(dtype=np.float64)
    value=joined[f'mean{window}'].to_numpy(dtype=np.float64)
    comparable=complete&np.isfinite(stored)&np.isfinite(value)
    same=np.isclose(stored,value,rtol=1e-6,atol=1e-6)
    results.append({'window':window,'comparable_rows':int(comparable.sum()),'equal_rows':int((comparable&same).sum()),'median_absolute_error':float(np.median(np.abs(stored[comparable]-value[comparable]))) if comparable.any() else None})
report={'purpose':'contract diagnostic, no model or teacher mutation','rows':len(target),'full_window_rule':'targets>=20100101 with history captured since20000101','raw_finish_membership':'original source, no official overlay in this diagnostic','results':results,'production_eligible':False}
(out/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
(out/'query.sql').write_text(sql,encoding='utf-8')
print('NVD_CORNER_CONTRACT',report,flush=True)
PY
