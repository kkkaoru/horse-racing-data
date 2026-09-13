#!/usr/bin/env bash
# Separate target-event fitting failure from development generalization failure; no fitting.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from catboost import CatBoostRanker

root=Path('research/jra-20260913')
out=root/'event-fit-diagnosis-002'
out.mkdir(exist_ok=False)
profiles={'uniform168':root/'priority-race-context-restoration-001','balanced168':root/'priority-event-weight-001'}
tasks=[]
union=set()
for profile,parent in profiles.items():
    for year in (2020,2021,2022,2023):
        cases=json.loads((parent/f'year-{year}/cohorts.json').read_text(encoding='utf-8'))
        for case in cases:
            event_ids=set(case['seed_race_ids'])&set(case['training_race_ids'])
            if not event_ids or event_ids&set(case['evaluation_race_ids']):
                raise ValueError('Prior target-event membership invalid')
            union.update(event_ids)
            tasks.append((profile,parent,year,case,event_ids))
features=json.loads((profiles['uniform168']/'year-2023/protocol.json').read_text(encoding='utf-8'))['feature_names']
pa.set_cpu_count(4)
pa.set_io_thread_count(4)
source=Path('/Users/kkk4oru/.local/share/horse-racing-data-production/jra-cell-v1/features-2000-2026-mssd-sectional-relationship-all-history-dedup-v2')
feature_frame=ds.dataset(source,format='parquet',partitioning='hive').to_table(columns=['race_id','ketto_toroku_bango','umaban',*features],filter=ds.field('race_id').isin(sorted(union))).to_pandas().rename(columns={'ketto_toroku_bango':'horse_id','umaban':'horse_number'})
status=ds.dataset(root/'training-status-002/jvd-runner-status.parquet').to_table(filter=ds.field('race_id').isin(sorted(union))).to_pandas()
status['horse_number']=pd.to_numeric(status['horse_number'],errors='raise').astype(np.int32)
status=status.loc[~status['abnormality_code'].isin(['1','2','3'])].copy()
status['actual_finish']=pd.to_numeric(status['finish_text'],errors='coerce')
status.loc[status['abnormality_code'].isin(['4','5']),'actual_finish']=np.nan
identity=['race_id','horse_id','horse_number']
frame=status.merge(feature_frame,on=identity,how='left',validate='one_to_one').sort_values(identity)
rows=[]
details=[]
for profile,parent,year,case,event_ids in tasks:
    selected=frame.loc[frame['race_id'].isin(event_ids)].copy()
    if set(selected['race_id'])!=event_ids:
        raise ValueError('Historical target event missing from source snapshot')
    model=CatBoostRanker()
    model.load_model(str(parent/f'year-{year}'/case['cell_id']/f'year-{year}/model.cbm'))
    selected['score']=model.predict(selected[features].to_numpy(dtype=np.float32),thread_count=4)
    outcomes=[]
    for race_id,group in selected.groupby('race_id',sort=True):
        ordered=group.sort_values(['score','horse_id','horse_number'],ascending=[False,True,True])
        winners=ordered.loc[ordered['actual_finish'].eq(1)]
        seconds=ordered.loc[ordered['actual_finish'].eq(2)]
        if len(winners)!=1 or len(seconds)!=1:
            raise ValueError('Ambiguous historical winner/runner-up requires review')
        winner=winners.iloc[0];second=seconds.iloc[0]
        outcome={'profile':profile,'year':year,'cell':case['cell_id'],'race_id':race_id,'winner_top1':bool(ordered.iloc[0]['actual_finish']==1),'winner_above_runnerup':bool(winner['score']>second['score']),'winner_grade_history_missing':bool(pd.isna(winner['same_grade_win_rate'])),'runnerup_grade_history_missing':bool(pd.isna(second['same_grade_win_rate'])),'evaluation_role':'in-sample diagnostic only, NOT accuracy improvement'}
        details.append(outcome);outcomes.append(outcome)
    missing_winners=[row for row in outcomes if row['winner_grade_history_missing']]
    rows.append({'profile':profile,'year':year,'cell':case['cell_id'],'prior_same_event_races':len(event_ids),'all_teacher_races':len(case['training_race_ids']),'same_event_race_fraction':len(event_ids)/len(case['training_race_ids']),'in_sample_event_top1_hits':sum(row['winner_top1'] for row in outcomes),'in_sample_winner_above_runnerup':sum(row['winner_above_runnerup'] for row in outcomes),'grade_history_missing_winner_races':len(missing_winners),'grade_history_missing_winner_top1':sum(row['winner_top1'] for row in missing_winners),'not_independent_validation':True})
(out/'summary.json').write_text(json.dumps(rows,indent=2),encoding='utf-8')
(out/'race-details.json').write_text(json.dumps(details,indent=2),encoding='utf-8')
print('EVENT_FIT_DIAGNOSIS',rows,flush=True)
PY
