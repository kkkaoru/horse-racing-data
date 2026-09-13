#!/usr/bin/env bash
# Restore absent age/career context using canonical window builders, with live parity.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export PYTHONPATH="$PWD/apps/pc-keiba-viewer/src/scripts:$PWD/apps/pc-keiba-viewer/src/scripts/finish-position-features:${PYTHONPATH:-}"
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import gzip
import hashlib
import json
import re
import runpy
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

root=Path('research/jra-20260913')
out=root/'nvd-race-context-restoration-003'
out.mkdir(exist_ok=False)
core_path=Path('apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py')
relative_path=core_path.parent/'finish-position-features/add-relationship-r1-features.py'
window_builder=runpy.run_path(str(core_path))['_window_query_from_base_table']
relative_builder=runpy.run_path(str(relative_path))['stage_race_relative']
if not callable(window_builder) or not callable(relative_builder):
    raise TypeError('Canonical context builders unavailable')
window_sql=window_builder('canonical_base')
if not isinstance(window_sql,str):
    raise TypeError('Canonical window SQL must be text')
career_names=['career_win_rate_rank_in_race','career_place_rate_rank_in_race','career_win_rate_diff_from_race_avg','career_place_rate_diff_from_race_avg']
names=['barei','futan_per_barei','barei_diff_from_race_mean',*career_names]
identity=['race_id','horse_id','horse_number']
keys=['kaisai_nen','kaisai_tsukihi','keibajo_code','race_bango']
target=pd.read_parquet(root/'nvd-corner-restoration-001/restored-teachers.parquet')
if not target[names].isna().all().all():
    raise ValueError('Expected seven still-unrestored columns')
with gzip.open(root/'nvd-full-roster-001/runners.json.gz','rt',encoding='utf-8') as stream:
    raw=pd.DataFrame(json.load(stream))
raw['race_id']='nar:'+raw['kaisai_nen']+':'+raw['kaisai_tsukihi']+':'+raw['keibajo_code']+':'+raw['race_bango']
raw['horse_id']=raw['ketto_toroku_bango']
raw['horse_number']=pd.to_numeric(raw['umaban'],errors='raise')
raw['barei']=pd.to_numeric(raw['barei'],errors='coerce').replace(0,np.nan)
raw['bataiju']=pd.to_numeric(raw['bataiju'],errors='coerce').replace(0,np.nan)
raw['futan_juryo']=pd.to_numeric(raw['futan_juryo'],errors='coerce').replace(0,np.nan)/10
teachers=target.drop(columns='barei').merge(raw[identity+['barei','bataiju','futan_juryo']],on=identity,how='left',validate='one_to_one')
live=pd.concat([pd.read_parquet(p) for p in sorted((root/'live-features-002').glob('*.parquet'))],ignore_index=True).rename(columns={'ketto_toroku_bango':'horse_id','umaban':'horse_number'})
# Current live body weight is unavailable and irrelevant to the two selected age outputs.
# Preserve its absence; never infer or supply a numeric body weight.
live=live.copy()
live['bataiju']=np.nan
results={}
for label,frame in [('live',live),('teachers',teachers)]:
    frame=frame.copy()
    parts=frame['race_id'].str.split(':',expand=True)
    frame['source']=parts[0]
    for index,key in enumerate(keys,start=1):
        frame[key]=parts[index]
    frame['ketto_toroku_bango']=frame['horse_id']
    required=sorted(set(re.findall(r'\bb\.([a-z][a-z0-9_]*)',window_sql))|{'race_id','horse_id','horse_number'})
    relation_keys=['source',*keys,'ketto_toroku_bango']
    with duckdb.connect() as con:
        con.execute('SET threads=4')
        con.register('canonical_base',frame[required])
        windows=con.execute(window_sql).fetchdf()
        con.register('base_input',frame[relation_keys+['futan_juryo','bataiju','barei','kyori']])
        relative_builder(con)
        relative=con.execute('SELECT * FROM race_relative').fetchdf()
    derived=frame[identity+relation_keys+['barei']].merge(relative[relation_keys+['futan_per_barei','barei_diff_from_race_mean']],on=relation_keys,how='left',validate='one_to_one').merge(windows[identity+career_names],on=identity,how='left',validate='one_to_one')
    results[label]=derived[identity+names]
parity=live[identity+names].merge(results['live'],on=identity,suffixes=('_stored','_derived'),validate='one_to_one')
checks=[]
for name in names:
    same=np.isclose(parity[name+'_stored'].to_numpy(dtype=np.float64),parity[name+'_derived'].to_numpy(dtype=np.float64),rtol=1e-6,atol=1e-6,equal_nan=True)
    checks.append({'feature':name,'rows':len(same),'equal_rows':int(same.sum())})
(out/'live-parity.json').write_text(json.dumps(checks,indent=2),encoding='utf-8')
if len(parity)!=314 or any(row['rows']!=row['equal_rows'] for row in checks):
    raise ValueError('Canonical live context parity failed')
derived=target[identity].merge(results['teachers'],on=identity,how='left',validate='one_to_one')
unknown_career_races=set(target.loc[~target['feature_row_present'].eq(True),'race_id'])
unknown_age_races=set(derived.loc[derived['barei'].isna(),'race_id'])
for name in career_names:
    derived.loc[derived['race_id'].isin(unknown_career_races),name]=np.nan
derived.loc[derived['race_id'].isin(unknown_age_races),'barei_diff_from_race_mean']=np.nan
restored=target.copy()
for name in names:
    restored[name]=derived[name].to_numpy()
unchanged=[name for name in target.columns if name not in names]
pd.testing.assert_frame_equal(restored[unchanged],target[unchanged],check_exact=True)
restored.to_parquet(out/'nvd-teachers.parquet',index=False)
(out/'canonical-window.sql').write_text(window_sql,encoding='utf-8')
report={'rows':len(restored),'changed_columns':names,'other_columns_exactly_preserved':True,'withheld_career_field_races':len(unknown_career_races),'withheld_age_field_races':len(unknown_age_races),'nonmissing_by_column':{name:int(restored[name].notna().sum()) for name in names},'live_parity':checks,'canonical_sources':{str(path):hashlib.sha256(path.read_bytes()).hexdigest() for path in (core_path,relative_path)},'unproven_body_weight_outputs_not_used':True,'production_eligible':False}
(out/'manifest.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print('NVD_RACE_CONTEXT_RESTORED',report,flush=True)
PY
