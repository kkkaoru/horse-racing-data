#!/usr/bin/env bash
# Stage a matched common-schema source ablation; not a claim of 168-feature restoration.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import gzip
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

root=Path('research/jra-20260913')
out=root/'common-schema-source-001'
out.mkdir(exist_ok=False)
original=json.loads((root/'priority-capacity-001/marketfree/year-2023/protocol.json').read_text(encoding='utf-8'))['feature_names']
features=pd.read_parquet(root/'nvd-full-roster-001/available-feature-rows.parquet')
names=[name for name in original if name in features.columns]
if len(names)!=117:
    raise ValueError('Unexpected common feature schema')
with gzip.open(root/'nvd-full-roster-001/runners.json.gz','rt',encoding='utf-8') as stream:
    se=pd.DataFrame(json.load(stream))
with gzip.open(root/'nvd-full-roster-001/races.json.gz','rt',encoding='utf-8') as stream:
    ra=pd.DataFrame(json.load(stream))
keys=['kaisai_nen','kaisai_tsukihi','keibajo_code','race_bango']
normal=se.merge(ra.loc[~ra['data_kubun'].str.strip().isin(['0','9']),keys],on=keys,validate='many_to_one')
active=normal.loc[~normal['ijo_kubun_code'].str.strip().isin(['1','2','3'])].copy()
active['race_id']='nar:'+active['kaisai_nen']+':'+active['kaisai_tsukihi']+':'+active['keibajo_code']+':'+active['race_bango']
active['horse_id']=active['ketto_toroku_bango'].str.strip()
active['horse_number']=pd.to_numeric(active['umaban'].str.strip(),errors='raise').astype(np.int32)
active['abnormality_code']=active['ijo_kubun_code'].str.strip()
active['source_status']=active['data_kubun'].str.strip()
active['finish_text']=active['kakutei_chakujun'].str.strip()
active['actual_finish']=pd.to_numeric(active['finish_text'],errors='coerce')
proof_path=root/'official-nvd-status-001/report.json'
proof=json.loads(proof_path.read_text(encoding='utf-8'))
if len(proof['matching_rows'])!=1 or not proof['matching_rows'][0].startswith('1 4 4 マイタイザン '):
    raise ValueError('Official normal-winner result must be explicitly verified')
if hashlib.sha256((proof_path.parent/'result.html').read_bytes()).hexdigest()!=proof['sha256']:
    raise ValueError('Official source hash mismatch')
mask=active['race_id'].eq('nar:2018:1019:50:10') & active['horse_id'].eq('2013102126') & active['horse_number'].eq(4)
if int(mask.sum())!=1 or active.loc[mask,'finish_text'].tolist()!=['00'] or active.loc[mask,'bamei'].str.strip().tolist()!=['マイタイザン']:
    raise ValueError('Original source identity or missing-outcome state changed')
active.loc[mask,'actual_finish']=1
active.loc[active['abnormality_code'].isin(['4','5']),'actual_finish']=np.nan
undefined=~active['abnormality_code'].isin(['4','5']) & ~active['actual_finish'].gt(0)
if undefined.any():
    raise ValueError('Unresolved normal outcome remains')
identity=['race_id','horse_id','horse_number']
features=features.rename(columns={'ketto_toroku_bango':'horse_id','umaban':'horse_number'})
features['horse_number']=pd.to_numeric(features['horse_number'],errors='raise').astype(np.int32)
features['feature_row_present']=True
frame=active[identity+['abnormality_code','source_status','finish_text','actual_finish']].merge(features[identity+names+['feature_row_present']],on=identity,how='left',validate='one_to_one')
frame['tansho_odds']=np.nan
if np.isinf(np.asarray(frame[names],dtype=np.float64)).any():
    raise ValueError('Infinite common-schema inputs')
frame.to_parquet(out/'nvd-teachers.parquet',index=False)
manifest={'comparison_plan':'JVD-only versus JVD+NVD using identical 117 common features; source effect must not be conflated with reducing 168 to 117','original_168_models_preserved':True,'not_full_168_feature_restoration':True,'feature_names':names,'unavailable_51_features':[name for name in original if name not in names],'rows':len(frame),'races':int(frame['race_id'].nunique()),'missing_feature_rows':int(frame['feature_row_present'].isna().sum()),'missing_feature_policy':'NaN, never zero-fill or drop active runners; pre-2006 inputs unavailable in retained NAR feature snapshot','noneligible_race_ledger':'nvd-rival-history-001/noneligible-race-ledger.json','official_label_overlay':{'race_id':'nar:2018:1019:50:10','horse_id':'2013102126','horse_number':4,'original_finish_text':'00','original_abnormality_code':'0','derived_actual_finish':1,'source':proof},'production_eligible':False}
(out/'manifest.json').write_text(json.dumps(manifest,indent=2,ensure_ascii=False),encoding='utf-8')
print('COMMON_SCHEMA_SOURCE_STAGED',manifest['rows'],manifest['races'],manifest['missing_feature_rows'],flush=True)
PY
