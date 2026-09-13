#!/usr/bin/env bash
# Resolve two duplicate-named feature producers before alleging a source defect.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export PYTHONPATH="$PWD/apps/pc-keiba-viewer/src/scripts:${PYTHONPATH:-}"
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import gzip
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from finish_position_features_duckdb import weight_cte

root=Path('research/jra-20260913')
out=root/'core-weight-contract-001'
out.mkdir(exist_ok=False)
base=pd.read_parquet(root/'jra-physics-contract-001/base.parquet')
with gzip.open(root/'jra-physics-contract-001/runners.json.gz','rt',encoding='utf-8') as stream:
    raw=pd.DataFrame(json.load(stream))
with gzip.open(root/'jra-margin-contract-002/raw-histories.json.gz','rt',encoding='utf-8') as stream:
    membership=pd.DataFrame(json.load(stream))
keys=['kaisai_nen','kaisai_tsukihi','keibajo_code','race_bango','ketto_toroku_bango','umaban']
raw=raw.merge(membership[keys+['has_ra']],on=keys,how='left',validate='one_to_one')
if raw['has_ra'].isna().any():
    raise ValueError('Raw snapshot identity mismatch')
with duckdb.connect() as con:
    con.execute('SET threads=4')
    con.register('targets',base)
    con.register('raw_runners',raw)
    con.execute("CREATE TEMP TABLE h AS SELECT *,strptime(kaisai_nen||kaisai_tsukihi,'%Y%m%d') AS race_dt,try_cast(nullif(trim(bataiju),'') AS INTEGER) AS weight FROM raw_runners WHERE has_ra AND coalesce(trim(data_kubun),'')<>'1' AND try_cast(umaban AS INTEGER) BETWEEN 1 AND 18 AND try_cast(kakutei_chakujun AS INTEGER)>0")
    sql="""
    CREATE TEMP TABLE horse_history_base AS
    SELECT t.source,t.kaisai_nen,t.kaisai_tsukihi,t.keibajo_code,t.race_bango,t.ketto_toroku_bango,
      h.weight AS history_bataiju,
      row_number() OVER(PARTITION BY t.race_id,t.ketto_toroku_bango,t.umaban ORDER BY h.race_dt DESC) AS recent_rank
    FROM targets t JOIN h ON h.ketto_toroku_bango=t.ketto_toroku_bango
      AND h.race_dt<strptime(t.race_date,'%Y%m%d')
      AND h.race_dt>=strptime(t.race_date,'%Y%m%d')-INTERVAL '10 years'
    """
    con.execute(sql)
    con.execute("CREATE TEMP TABLE target_current_bataiju AS SELECT t.source,t.kaisai_nen,t.kaisai_tsukihi,t.keibajo_code,t.race_bango,t.ketto_toroku_bango,try_cast(nullif(trim(r.bataiju),'') AS INTEGER) AS current_bataiju,try_cast(nullif(trim(r.zogen_sa),'') AS INTEGER) AS target_zogen_sa FROM targets t LEFT JOIN raw_runners r ON t.race_id='jra:'||r.kaisai_nen||':'||r.kaisai_tsukihi||':'||r.keibajo_code||':'||r.race_bango AND t.ketto_toroku_bango=r.ketto_toroku_bango AND t.umaban=try_cast(r.umaban AS INTEGER)")
    cte=weight_cte()
    derived=con.execute('WITH '+cte+" SELECT 'jra:'||kaisai_nen||':'||kaisai_tsukihi||':'||keibajo_code||':'||race_bango AS race_id,ketto_toroku_bango,weight_trend_5,weight_volatility_5 FROM weight_agg").fetchdf()
joined=base.merge(derived,on=['race_id','ketto_toroku_bango'],how='left',suffixes=('_stored','_derived'),validate='one_to_one')
joined.to_parquet(out/'comparison.parquet',index=False)
results=[]
for name in ('weight_trend_5','weight_volatility_5'):
    same=np.isclose(joined[name+'_stored'].to_numpy(dtype=np.float64),joined[name+'_derived'].to_numpy(dtype=np.float64),rtol=1e-6,atol=1e-6,equal_nan=True)
    results.append({'feature':name,'rows':len(same),'equal_rows':int(same.sum()),'mismatched_identities':joined.loc[~same,['race_id','ketto_toroku_bango']].to_dict(orient='records')})
report={'producer':'actual core weight_cte, not sectional replacement','reason':'sectional append preserves existing same-name core columns','results':results,'production_eligible':False}
(out/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
(out/'query.sql').write_text(sql+';\n'+cte,encoding='utf-8')
print('CORE_WEIGHT_CONTRACT',report,flush=True)
PY
