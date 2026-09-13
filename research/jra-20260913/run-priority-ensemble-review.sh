#!/usr/bin/env bash
# One fixed model-only average; no market features and no additional fitting.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export PYTHONPATH="$PWD/research/jra-20260913"
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import json
from dataclasses import asdict
from pathlib import Path

import numpy as np
import pandas as pd
from race_rank_review import review_orders

root=Path('research/jra-20260913')
out=root/'priority-ensemble-001'
out.mkdir(exist_ok=False)
cells=('jra-event-nakayama-st-lite-2200-v1','jra-event-hanshin-challenge-2000-v1')
(out/'freeze.json').write_text(json.dumps({'members':['priority-capacity-001','priority-recency-001'],'weights':[0.5,0.5],'years':[2020,2021,2022,2023],'cells':cells,'reason':'one fixed long-history/recently-weighted average to test complementary model errors','source_scores':'retained JSON scores; native full-precision replay required before promotion','market_input':False,'production_eligible':False},indent=2),encoding='utf-8')
reports=[]
for cell in cells:
    annual=[]
    for year in (2020,2021,2022,2023):
        a=pd.read_json(root/f'priority-capacity-001/marketfree/year-{year}/{cell}/year-{year}/predictions.json')
        b=pd.read_json(root/f'priority-recency-001/marketfree/year-{year}/{cell}/year-{year}/predictions.json')
        keys=['race_id','horse_id','horse_number']
        joined=a.merge(b,on=keys,suffixes=('_a','_b'),validate='one_to_one')
        if len(joined)!=len(a) or len(a)!=len(b):
            raise ValueError('Member roster mismatch')
        if not ((joined['actual_finish_a']==joined['actual_finish_b']) | (joined['actual_finish_a'].isna()&joined['actual_finish_b'].isna())).all():
            raise ValueError('Member label mismatch')
        joined['score']=.5*joined['model_score_a']+.5*joined['model_score_b']
        rows=[]
        for race_id,group in joined.groupby('race_id'):
            order=tuple(int(x) for x in group.sort_values(['score','horse_id','horse_number'],ascending=[False,True,True])['horse_number'])
            actual={int(h):int(f) if np.isfinite(f) else None for h,f in zip(group['horse_number'],group['actual_finish_a'],strict=True)}
            odds=np.asarray(group['tansho_odds_a'],dtype=np.float64)
            if not np.isfinite(odds).all() or (odds<=0).any():
                raise ValueError('Incomplete market comparison')
            market=tuple(int(x) for x in group.sort_values(['tansho_odds_a','horse_number'])['horse_number'])
            rows.append({'race_id':race_id,'order':order,**asdict(review_orders(predicted=order,actual=actual,market=market))})
        delta=[sum(row['model_minus_market'][k] for row in rows) for k in range(5)]
        annual.append({'year':year,'delta':delta,'races':rows})
    aggregate=[sum(row['delta'][k] for row in annual) for k in range(5)]
    report={'cell':cell,'aggregate':aggregate,'annual':annual,'development_guard':aggregate[0]>0 and all(value>=0 for row in annual for value in row['delta'][1:]),'production_eligible':False}
    reports.append(report)
    print('ENSEMBLE',cell,aggregate,'guard',report['development_guard'],flush=True)
(out/'summary.json').write_text(json.dumps(reports,indent=2),encoding='utf-8')
PY
