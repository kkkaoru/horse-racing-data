#!/usr/bin/env bash
# Restore missing margin inputs with canonical history membership and censoring.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export PYTHONPATH="$PWD/research/jra-20260913:$PWD/apps/pc-keiba-viewer/src/scripts:${PYTHONPATH:-}"
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import gzip
import hashlib
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from finish_position_features_duckdb import race_context_cte
from margin_features import MARGIN_FEATURE_NAMES, restore_margin_features

root=Path('research/jra-20260913')
out=root/'nvd-margin-restoration-001'
out.mkdir(exist_ok=False)
target=pd.read_parquet(root/'common-schema-source-001/nvd-teachers.parquet')
keys=['kaisai_nen','kaisai_tsukihi','keibajo_code','race_bango']
identity=['race_id','horse_id','horse_number']
with gzip.open(root/'nvd-rival-history-001/nvd-runner-history.json.gz','rt',encoding='utf-8') as stream:
    records=json.load(stream)
raw=pd.DataFrame.from_records(records,columns=[*keys,'ketto_toroku_bango','umaban','time_sa','kakutei_chakujun','ijo_kubun_code'])
del records
with gzip.open(root/'nvd-history-race-metadata-001/races.json.gz','rt',encoding='utf-8') as stream:
    metadata=pd.DataFrame(json.load(stream))
raw['umaban']=pd.to_numeric(raw['umaban'],errors='raise')
if raw.duplicated([*keys,'ketto_toroku_bango','umaban']).any():
    raise ValueError('Duplicate prior runner identities')
raw=raw.merge(metadata[keys],on=keys,how='left',validate='many_to_one',indicator=True)
raw.loc[raw['_merge'].eq('left_only')].drop(columns='_merge').to_parquet(out/'orphan-prior-runners.parquet',index=False)
orphan_count=int(raw['_merge'].eq('left_only').sum())
raw=raw.loc[raw['_merge'].eq('both')].drop(columns='_merge').copy()
proof=json.loads((root/'common-schema-source-001/manifest.json').read_text(encoding='utf-8'))['official_label_overlay']
mask=raw['kaisai_nen'].eq('2018')&raw['kaisai_tsukihi'].eq('1019')&raw['keibajo_code'].eq('50')&raw['race_bango'].eq('10')&raw['ketto_toroku_bango'].eq('2013102126')&raw['umaban'].eq(4)
if int(mask.sum())!=1 or raw.loc[mask,'kakutei_chakujun'].tolist()!=['00'] or proof['derived_actual_finish']!=1:
    raise ValueError('Verified historical label overlay changed')
raw.loc[mask,'kakutei_chakujun']='01'
finished=raw.loc[pd.to_numeric(raw['kakutei_chakujun'],errors='coerce').gt(0)]
ties=finished.loc[finished.duplicated(['ketto_toroku_bango','kaisai_nen','kaisai_tsukihi'],keep=False)]
ties.to_parquet(out/'same-day-history-ties.parquet',index=False)
if not ties.empty:
    raise ValueError('Same-day history ties need chronological review')
with duckdb.connect() as con:
    con.execute('SET threads=4')
    con.execute("SET memory_limit='2GB'")
    con.execute(f"SET temp_directory='{out.as_posix()}/scratch'")
    con.register('targets',target)
    con.register('history',raw)
    sql="""
    WITH h AS (
      SELECT *,kaisai_nen||kaisai_tsukihi AS race_date,
       strptime(kaisai_nen||kaisai_tsukihi,'%Y%m%d') AS race_dt,
       try_cast(nullif(nullif(trim(time_sa),''),'0000') AS DOUBLE)/10 AS margin
      FROM history WHERE try_cast(kakutei_chakujun AS INTEGER)>0
    ), ranked AS (
      SELECT t.race_id,t.horse_id,t.horse_number,h.margin,
       row_number() OVER(PARTITION BY t.race_id,t.horse_id,t.horse_number ORDER BY h.race_date DESC) AS rn
      FROM targets t JOIN h ON t.horse_id=h.ketto_toroku_bango
       AND h.race_date<substr(t.race_id,5,4)||substr(t.race_id,10,4)
       AND h.race_dt>=strptime(substr(t.race_id,5,4)||substr(t.race_id,10,4),'%Y%m%d')-INTERVAL '10 years'
    ) SELECT race_id,horse_id,horse_number,avg(margin) AS mean_margin,
       min(margin) AS best_margin,max(margin) FILTER(WHERE rn=1) AS last_margin,
       count(*) AS prior_count
      FROM ranked WHERE rn<=5 GROUP BY race_id,horse_id,horse_number
    """
    derived=con.execute(sql).fetchdf()
joined=target[identity].merge(derived,on=identity,how='left',validate='one_to_one')
dates=joined['race_id'].str.slice(4,8)+joined['race_id'].str.slice(9,13)
if dates.gt('20221231').any():
    raise ValueError('Targets exceed captured history horizon')
incomplete=dates.lt('20100101')&joined['prior_count'].fillna(0).lt(5)
features=restore_margin_features(joined['race_id'].tolist(),mean_margin=np.asarray(joined['mean_margin'],dtype=np.float64),best_margin=np.asarray(joined['best_margin'],dtype=np.float64),last_margin=np.asarray(joined['last_margin'],dtype=np.float64),incomplete_window=np.asarray(incomplete,dtype=np.bool_))
restored=target.copy()
for name,values in features.items():
    restored[name]=values
unchanged=[name for name in target.columns if name not in MARGIN_FEATURE_NAMES]
pd.testing.assert_frame_equal(restored[unchanged],target[unchanged],check_exact=True)
# Independently exercise the canonical public field aggregation SQL.
career=pd.DataFrame({'source':'nar','kaisai_nen':dates.str.slice(0,4),'kaisai_tsukihi':dates.str.slice(4,8),'keibajo_code':target['race_id'].str.split(':').str[3],'race_bango':target['race_id'].str.split(':').str[4],'speed_index_avg_5':restored['speed_index_avg_5'],'speed_index_best_5':restored['speed_index_best_5'],'same_distance_win_rate':target['same_distance_win_rate']})
with duckdb.connect() as con:
    con.register('horse_career',career)
    context_sql=race_context_cte()
    check=con.execute('WITH '+context_sql+" SELECT 'nar:'||a.kaisai_nen||':'||a.kaisai_tsukihi||':'||a.keibajo_code||':'||a.race_bango AS race_id,a.race_avg_speed,b.race_top_speed FROM race_field_aggregates a LEFT JOIN race_top3_speed b USING(source,kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)").fetchdf().set_index('race_id')
blocked_races=set(joined.loc[incomplete,'race_id'])
complete=~joined['race_id'].isin(blocked_races)
for own,reference in [('field_strength_avg_speed','race_avg_speed'),('field_strength_top3_speed','race_top_speed')]:
    actual=restored.loc[complete,own].to_numpy(dtype=np.float64)
    expected=joined.loc[complete,'race_id'].map(check[reference]).to_numpy(dtype=np.float64)
    if not np.allclose(actual,expected,rtol=1e-12,atol=1e-12,equal_nan=True):
        raise ValueError('Canonical race aggregation mismatch')
restored.to_parquet(out/'nvd-teachers.parquet',index=False)
joined.assign(incomplete_window=incomplete).to_parquet(out/'derived-history-audit.parquet',index=False)
(out/'query.sql').write_text(sql,encoding='utf-8')
(out/'canonical-context.sql').write_text(context_sql,encoding='utf-8')
inputs={}
for path in [root/'common-schema-source-001/nvd-teachers.parquet',root/'nvd-rival-history-001/nvd-runner-history.json.gz',root/'nvd-history-race-metadata-001/races.json.gz']:
    with path.open('rb') as stream:
        inputs[str(path)]=hashlib.file_digest(stream,'sha256').hexdigest()
report={'rows':len(restored),'races':int(restored['race_id'].nunique()),'orphan_prior_rows_preserved':orphan_count,'incomplete_windows':int(incomplete.sum()),'withheld_field_races':len(blocked_races),'canonical_field_parity_rows':int(complete.sum()),'changed_columns':MARGIN_FEATURE_NAMES,'nonmissing_after':{name:int(restored[name].notna().sum()) for name in MARGIN_FEATURE_NAMES},'other_columns_exactly_preserved':True,'official_history_membership_overlay':proof,'inputs':inputs,'production_eligible':False}
(out/'manifest.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print('NVD_MARGIN_RESTORED',report,flush=True)
PY
