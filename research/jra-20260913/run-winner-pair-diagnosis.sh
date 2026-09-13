#!/usr/bin/env bash
# Diagnose retained predictions and native pair contributions; never fit or tune.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import gzip
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from catboost import CatBoostRanker, Pool

root=Path('research/jra-20260913')
out=root/'winner-pair-diagnosis-001'
out.mkdir(exist_ok=False)
profiles={
 'original168':root/'priority-capacity-001/marketfree',
 'common117-jvd':root/'priority-common-source-001/jvd-only',
 'common117-mixed':root/'priority-common-source-001/jvd-plus-nvd',
 'margin117':root/'priority-margin-restoration-001',
 'corner168':root/'priority-corner-restoration-001/restored',
 'latest168':root/'priority-race-context-restoration-001',
 'classifier168':root/'priority-capacity-001/position',
}
cells=['jra-event-nakayama-st-lite-2200-v1','jra-event-hanshin-challenge-2000-v1']
race_ids=json.loads((root/'jra-margin-contract-002/report.json').read_text(encoding='utf-8'))['races']
features=json.loads((profiles['original168']/'year-2023/protocol.json').read_text(encoding='utf-8'))['feature_names']
pa.set_cpu_count(4)
pa.set_io_thread_count(4)
source=Path('/Users/kkk4oru/.local/share/horse-racing-data-production/jra-cell-v1/features-2000-2026-mssd-sectional-relationship-all-history-dedup-v2')
feature_frame=ds.dataset(source,format='parquet',partitioning='hive').to_table(columns=['race_id','ketto_toroku_bango','umaban',*features],filter=ds.field('race_id').isin(race_ids)).to_pandas().rename(columns={'ketto_toroku_bango':'horse_id','umaban':'horse_number'})
with gzip.open(root/'jra-physics-contract-001/runners.json.gz','rt',encoding='utf-8') as stream:
    runners=json.load(stream)
horse_names={str(row['ketto_toroku_bango']):str(row['bamei']).strip() for row in runners if row.get('bamei')}
rows=[]
contributions=[]
input_hashes={}
for profile,parent in profiles.items():
    for year in (2020,2021,2022,2023):
        protocol=json.loads((parent/f'year-{year}/protocol.json').read_text(encoding='utf-8'))
        for cell in cells:
            folder=parent/f'year-{year}'/cell/f'year-{year}'
            prediction_path=folder/'predictions.json'
            predictions=pd.read_json(prediction_path,dtype={'race_id':str,'horse_id':str})
            input_hashes[str(prediction_path)]=hashlib.sha256(prediction_path.read_bytes()).hexdigest()
            if predictions['race_id'].nunique()!=1 or predictions['horse_number'].duplicated().any():
                raise ValueError('Expected one complete unique event field')
            actual_winner=np.flatnonzero(predictions['actual_finish'].eq(1).to_numpy())
            actual_runnerup=np.flatnonzero(predictions['actual_finish'].eq(2).to_numpy())
            if len(actual_winner)!=1 or len(actual_runnerup)!=1:
                raise ValueError('Winner/runner-up ambiguity requires a separate review')
            wi=int(actual_winner[0]);si=int(actual_runnerup[0])
            ordered=predictions.sort_values(['model_score','horse_id','horse_number'],ascending=[False,True,True])
            winner=predictions.iloc[wi];runnerup=predictions.iloc[si];choice=ordered.iloc[0]
            ranks={int(number):index for index,number in enumerate(ordered['horse_number'],start=1)}
            scores=predictions['model_score'].to_numpy(dtype=np.float64)
            market=predictions.loc[predictions['tansho_odds'].gt(0)].sort_values(['tansho_odds','horse_id','horse_number'])
            if len(market)!=len(predictions):
                raise ValueError('Incomplete market field in pair diagnosis')
            row={'profile':profile,'cell':cell,'year':year,'race_id':winner['race_id'],'winner_number':int(winner['horse_number']),'winner_name':horse_names.get(winner['horse_id'],winner['horse_id']),'runnerup_number':int(runnerup['horse_number']),'runnerup_name':horse_names.get(runnerup['horse_id'],runnerup['horse_id']),'winner_predicted_rank':ranks[int(winner['horse_number'])],'runnerup_predicted_rank':ranks[int(runnerup['horse_number'])],'top_choice_name':horse_names.get(choice['horse_id'],choice['horse_id']),'top_choice_actual_finish':None if pd.isna(choice['actual_finish']) else int(choice['actual_finish']),'winner_above_runnerup':bool(winner['model_score']>runnerup['model_score']),'market_top_actual_finish':None if pd.isna(market.iloc[0]['actual_finish']) else int(market.iloc[0]['actual_finish']),'prediction_order':ordered['horse_number'].astype(int).tolist(),'actual_finishes_in_predicted_order':[None if pd.isna(value) else int(value) for value in ordered['actual_finish']],'native_refit':False}
            if profile=='classifier168':
                probabilities=np.load(folder/'position-probabilities.npy',allow_pickle=False)
                if probabilities.shape!=(len(predictions),6):
                    raise ValueError('Classifier probabilities are not row-aligned')
                row['winner_probabilities']=probabilities[wi].tolist()
                row['runnerup_probabilities']=probabilities[si].tolist()
                row['score_semantics']='ordinal assignment, not confidence'
            else:
                row['winner_minus_runnerup_score']=float(scores[wi]-scores[si])
                row['field_score_std']=float(scores.std())
            rows.append(row)
            if profile not in ('original168','latest168'):
                continue
            aligned=predictions[['race_id','horse_id','horse_number']].merge(feature_frame,on=['race_id','horse_id','horse_number'],how='left',validate='one_to_one')
            matrix=aligned[protocol['feature_names']].to_numpy(dtype=np.float32)
            model=CatBoostRanker()
            model.load_model(str(folder/'model.cbm'))
            native=np.asarray(model.predict(matrix,thread_count=4),dtype=np.float64)
            if not np.allclose(native,scores,rtol=1e-8,atol=1e-8):
                raise ValueError('Native prediction/feature alignment failed')
            shap=np.asarray(model.get_feature_importance(Pool(matrix),type='ShapValues',thread_count=4),dtype=np.float64)
            if not np.allclose(shap.sum(axis=1),native,rtol=1e-8,atol=1e-8):
                raise ValueError('SHAP additive reconstruction failed')
            np.save(out/f'{profile}-{year}-{cell}-shap.npy',shap,allow_pickle=False)
            delta=shap[wi,:-1]-shap[si,:-1]
            drivers=[]
            for index in np.argsort(-np.abs(delta))[:12]:
                drivers.append({'feature':protocol['feature_names'][int(index)],'winner_value':float(matrix[wi,index]) if np.isfinite(matrix[wi,index]) else None,'runnerup_value':float(matrix[si,index]) if np.isfinite(matrix[si,index]) else None,'winner_minus_runnerup_contribution':float(delta[index])})
            contributions.append({'profile':profile,'cell':cell,'year':year,'score_gap':float(native[wi]-native[si]),'pair_contribution_sum':float(delta.sum()),'winner_missing_features':int(np.isnan(matrix[wi]).sum()),'runnerup_missing_features':int(np.isnan(matrix[si]).sum()),'largest_model_contributions':drivers,'warning':'Model attribution, not causal evidence and not a feature-removal recommendation'})
summary=[]
for profile in profiles:
    for cell in cells:
        selected=[row for row in rows if row['profile']==profile and row['cell']==cell]
        summary.append({'profile':profile,'cell':cell,'races':len(selected),'winner_top1':sum(row['winner_predicted_rank']==1 for row in selected),'actual_runnerup_chosen_top1':sum(row['runnerup_predicted_rank']==1 for row in selected),'winner_above_runnerup':sum(row['winner_above_runnerup'] for row in selected),'top1_other_than_first_or_second':sum(row['winner_predicted_rank']!=1 and row['runnerup_predicted_rank']!=1 for row in selected)})
for name,value in [('pairs',rows),('contributions',contributions),('summary',summary),('input-hashes',input_hashes)]:
    (out/f'{name}.json').write_text(json.dumps(value,indent=2,ensure_ascii=False,allow_nan=False),encoding='utf-8')
print('WINNER_PAIR_DIAGNOSIS',summary,flush=True)
PY
