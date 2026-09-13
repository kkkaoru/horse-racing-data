#!/usr/bin/env bash
# Reuse canonical corner aggregation; preserve shared inputs and label overlays.
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
from finish_position_features_duckdb import horse_running_style_history_cte

root=Path('research/jra-20260913')
out=root/'nvd-corner-restoration-001'
out.mkdir(exist_ok=False)
manifest=json.loads((root/'common-schema-source-001/manifest.json').read_text(encoding='utf-8'))
names=[n for n in manifest['unavailable_51_features'] if any(f'corner_{c}_norm' in n for c in (2,3,4))]
if len(names)!=24:
    raise ValueError('Expected24 absent corner2..4 features')
target=pd.read_parquet(root/'nvd-margin-restoration-001/nvd-teachers.parquet')
keys=['kaisai_nen','kaisai_tsukihi','keibajo_code','race_bango']
with gzip.open(root/'nvd-rival-history-001/nvd-runner-history.json.gz','rt',encoding='utf-8') as stream:
    records=json.load(stream)
raw=pd.DataFrame.from_records(records,columns=[*keys,'ketto_toroku_bango','umaban','kakutei_chakujun','kohan_3f','corner_1','corner_2','corner_3','corner_4'])
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
    corner_sql=','.join(f"CASE WHEN try_cast(m.shusso_tosu AS INTEGER)>1 THEN (try_cast(nullif(nullif(trim(r.corner_{c}),''),'00') AS DOUBLE)-1)/(try_cast(m.shusso_tosu AS INTEGER)-1) ELSE NULL END AS corner{c}_norm" for c in (1,2,3,4))
    history_sql="""CREATE TEMP TABLE h AS SELECT r.*,strptime(r.kaisai_nen||r.kaisai_tsukihi,'%Y%m%d') AS race_dt,try_cast(m.shusso_tosu AS INTEGER) AS field_size,try_cast(m.kyori AS INTEGER) AS history_kyori,m.track_code AS history_track_code,m.grade_code AS history_grade_code,try_cast(r.kakutei_chakujun AS INTEGER) AS finish_position,try_cast(nullif(nullif(trim(r.kohan_3f),''),'000') AS DOUBLE)/10 AS kohan_3f_seconds,"""+corner_sql+" FROM raw_history r JOIN metadata m USING(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango) WHERE try_cast(r.kakutei_chakujun AS INTEGER)>0 AND try_cast(m.kyori AS INTEGER) IS NOT NULL"
    con.execute(history_sql)
    invalid=con.execute('SELECT * FROM h WHERE field_size IS NULL OR field_size<=0').fetchdf()
    if not invalid.empty:
        invalid.to_parquet(out/'needs-full-peer-fallback.parquet',index=False)
        raise ValueError('Cannot use partial horse cohort for field-size fallback')
    base_sql="""
    CREATE TEMP TABLE horse_history_base AS
    SELECT 'nar' AS source,m.kaisai_nen,m.kaisai_tsukihi,m.keibajo_code,m.race_bango,
      t.horse_id AS ketto_toroku_bango,
      h.corner1_norm,h.corner2_norm,h.corner3_norm,h.corner4_norm,
      h.finish_position,h.kohan_3f_seconds AS kohan_3f,
      h.history_kyori,try_cast(m.kyori AS INTEGER) AS target_kyori,
      h.history_track_code,m.track_code AS target_track_code,
      h.keibajo_code AS history_keibajo,m.keibajo_code AS target_keibajo,
      h.history_grade_code,m.grade_code AS target_grade_code,
      row_number() OVER(PARTITION BY t.race_id,t.horse_id,t.horse_number ORDER BY h.race_dt DESC,h.keibajo_code,h.race_bango) AS recent_rank
    FROM targets t JOIN metadata m ON t.race_id='nar:'||m.kaisai_nen||':'||m.kaisai_tsukihi||':'||m.keibajo_code||':'||m.race_bango
    JOIN h ON h.ketto_toroku_bango=t.horse_id
      AND h.race_dt<strptime(m.kaisai_nen||m.kaisai_tsukihi,'%Y%m%d')
      AND h.race_dt>=strptime(m.kaisai_nen||m.kaisai_tsukihi,'%Y%m%d')-INTERVAL '10 years'
    """
    con.execute(base_sql)
    cte=horse_running_style_history_cte()
    derived=con.execute('WITH '+cte+" SELECT 'nar:'||kaisai_nen||':'||kaisai_tsukihi||':'||keibajo_code||':'||race_bango AS race_id,ketto_toroku_bango AS horse_id,"+','.join(names)+',past_corner_1_norm_avg_5 FROM horse_running_style_history').fetchdf()
    counts=con.execute("SELECT 'nar:'||kaisai_nen||':'||kaisai_tsukihi||':'||keibajo_code||':'||race_bango AS race_id,ketto_toroku_bango AS horse_id,count(*) AS prior_count FROM horse_history_base GROUP BY source,kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,ketto_toroku_bango").fetchdf()
identity=['race_id','horse_id']
joined=target[identity].merge(derived,on=identity,how='left',validate='one_to_one').merge(counts,on=identity,how='left',validate='one_to_one')
dates=target['race_id'].str.slice(4,8)+target['race_id'].str.slice(9,13)
complete=dates.ge('20100101')
reference=target.loc[complete,'past_corner_1_norm_avg_5'].to_numpy(dtype=np.float64)
actual=joined.loc[complete,'past_corner_1_norm_avg_5'].to_numpy(dtype=np.float64)
comparable=np.isfinite(reference)&np.isfinite(actual)
if not np.allclose(reference[comparable],actual[comparable],rtol=1e-6,atol=1e-6):
    raise ValueError('Canonical first-corner control does not reproduce shared snapshot')
control=target.copy()
for name in manifest['unavailable_51_features']:
    control[name]=np.nan
restored=control.copy()
censored={}
for name in names:
    window=1 if name.startswith('last_race_') else int(name.rsplit('_',1)[1])
    unknown=~complete&joined['prior_count'].fillna(0).lt(window)
    restored[name]=joined[name].mask(unknown)
    censored[name]=int(unknown.sum())
pd.testing.assert_frame_equal(restored[target.columns],target,check_exact=True)
control.to_parquet(out/'control-teachers.parquet',index=False)
restored.to_parquet(out/'restored-teachers.parquet',index=False)
(out/'history.sql').write_text(history_sql+';\n'+base_sql,encoding='utf-8')
(out/'canonical-corner.sql').write_text(cte,encoding='utf-8')
report={'rows':len(restored),'new_corner_columns':names,'remaining_unrestored_168_columns':[n for n in manifest['unavailable_51_features'] if n not in names],'shared_columns_exactly_unchanged':True,'first_corner_mean5_control_pairs':int(comparable.sum()),'raw_history_membership':'original raw positive finish, matching existing shared corner features; official teacher label remains1 but no new corner history overlay','censored_by_column':censored,'nonmissing_by_column':{n:int(restored[n].notna().sum()) for n in names},'matched_control':'same margin-restored117 plus51NaN columns; treatment changes24only','production_eligible':False}
(out/'manifest.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print('NVD_CORNERS_RESTORED',report,flush=True)
PY
