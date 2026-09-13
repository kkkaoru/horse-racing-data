#!/usr/bin/env bash
# Fixed decoding-only comparison on already trained native probability arrays.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export PYTHONPATH="$PWD/research/jra-20260913"
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import hashlib
import json
from dataclasses import asdict
from pathlib import Path

import numpy as np
import pandas as pd
from position_classifier import rank_scores_for_races
from race_rank_review import review_orders

root=Path('research/jra-20260913')
out=root/'joint-position-review-001'
out.mkdir(exist_ok=False)
(out/'position_classifier.py').write_bytes((root/'position_classifier.py').read_bytes())
(out/'freeze.json').write_text(json.dumps({'hypothesis':'hard-locking the maximum-P1 horse can prevent correctly assigning that same horse to second; test joint expected exact-hit utility instead','winner_weights':[1,2],'other_rank_weights':1,'years':[2020,2021,2022,2023],'training':'no new fitting; retained500-iteration six-class probabilities','market_input':False,'old_greedy_decoder_preserved':True,'production_eligible':False},indent=2),encoding='utf-8')
results=[]
for weight in (1,2):
    for cell in ('jra-event-nakayama-st-lite-2200-v1','jra-event-hanshin-challenge-2000-v1'):
        annual=[]
        for year in (2020,2021,2022,2023):
            source=root/f'priority-capacity-001/position/year-{year}/{cell}/year-{year}'
            frame=pd.read_json(source/'predictions.json')
            probabilities=np.load(source/'position-probabilities.npy',allow_pickle=False)
            race_ids=np.asarray(frame['race_id'],dtype=np.str_)
            original=rank_scores_for_races(probabilities,race_ids)
            if not np.array_equal(original,np.asarray(frame['model_score'],dtype=np.float64)):
                raise ValueError('Saved probability row alignment or legacy decoder changed')
            frame['joint_score']=rank_scores_for_races(probabilities,race_ids,winner_weight=weight)
            reviews=[]
            for race_id,group in frame.groupby('race_id'):
                order=tuple(int(n) for n in group.sort_values('joint_score',ascending=False)['horse_number'])
                actual={int(n):int(f) if np.isfinite(f) else None for n,f in zip(group['horse_number'],group['actual_finish'],strict=True)}
                odds=np.asarray(group['tansho_odds'],dtype=np.float64)
                if not np.isfinite(odds).all() or (odds<=0).any():
                    raise ValueError('Market reference is incomplete')
                market=tuple(int(n) for n in group.sort_values(['tansho_odds','horse_number'])['horse_number'])
                reviews.append({'race_id':race_id,'order':order,**asdict(review_orders(predicted=order,actual=actual,market=market))})
            delta=[sum(row['model_minus_market'][k] for row in reviews) for k in range(5)]
            annual.append({'year':year,'delta':delta,'reviews':reviews,'probability_sha256':hashlib.sha256((source/'position-probabilities.npy').read_bytes()).hexdigest()})
        aggregate=[sum(row['delta'][k] for row in annual) for k in range(5)]
        result={'cell':cell,'winner_weight':weight,'annual':annual,'aggregate':aggregate,'development_guard':aggregate[0]>0 and all(value>=0 for row in annual for value in row['delta'][1:]),'production_eligible':False}
        results.append(result)
        print('JOINT_POSITION',cell,weight,aggregate,result['development_guard'],flush=True)
(out/'summary.json').write_text(json.dumps(results,indent=2),encoding='utf-8')
PY
