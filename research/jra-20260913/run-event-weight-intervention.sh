#!/usr/bin/env bash
# One predeclared training-population intervention; retain every teacher race.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import ast
import hashlib
import json
from pathlib import Path

root=Path('research/jra-20260913')
out=root/'priority-event-weight-001'
out.mkdir(exist_ok=False)
archive=out/'source-code'
archive.mkdir()
fragment="""
    event_ids=set(case['seed_race_ids']) & set(case['training_race_ids'])
    group_weights=balanced_event_weights(np.asarray(train['race_id'],dtype=np.str_),event_ids)
    weight_table=pd.DataFrame({'race_id':train['race_id'].to_numpy(),'group_weight':group_weights}).drop_duplicates()
    if len(weight_table)!=train['race_id'].nunique() or not np.all(group_weights>0):
        raise ValueError('Every training group must retain one positive weight')
    event_mass=float(weight_table.loc[weight_table['race_id'].isin(event_ids),'group_weight'].sum())
    other_mass=float(weight_table.loc[~weight_table['race_id'].isin(event_ids),'group_weight'].sum())
    if not np.isclose(event_mass,other_mass,rtol=1e-12,atol=1e-12):
        raise ValueError('Frozen50:50 group-mass policy changed')
    weight_table.to_json(OUT/f'{cell_id}-group-weights.json',orient='records',indent=2)
    print('EVENT_GROUP_MASS',cell_id,len(event_ids),len(weight_table),event_mass,other_mass,flush=True)
"""
drivers=[]
for year in (2020,2021,2022,2023):
    name=f'run-event-weight-{year}-001.sh'
    script=(root/f'run-race-context-restoration-{year}-001.sh').read_text(encoding='utf-8')
    script=script.replace(f'priority-race-context-restoration-001/year-{year}',f'priority-event-weight-001/year-{year}')
    script=script.replace('from race_rank_review import review_orders','from race_rank_review import review_orders\nfrom event_weights import balanced_event_weights')
    marker="    print('FIT_START'"
    if script.count(marker)!=1:
        raise ValueError('Unexpected parent fit entry')
    script=script.replace(marker,fragment+'\n'+marker)
    old='output=target,config=CONFIG)'
    if script.count(old)!=1:
        raise ValueError('Unexpected native fit call')
    script=script.replace(old,'output=target,config=CONFIG,group_weights=group_weights)')
    script=script.replace("'market_used_in_training':False","'group_weight_policy':'50:50 mass over unique prior same-event and other teacher races; positive all groups','market_used_in_training':False")
    ast.parse(script.split("<<'PY'\n",1)[1].rsplit('\nPY',1)[0])
    with (root/name).open('x',encoding='utf-8') as stream:
        stream.write(script)
    (archive/name).write_text(script,encoding='utf-8')
    drivers.append({'year':year,'path':name,'sha256':hashlib.sha256(script.encode()).hexdigest()})
for name in ('fold_ranker.py','dedicated_cells.py','race_rank_review.py','event_weights.py'):
    (archive/name).write_bytes((root/name).read_bytes())
(out/'freeze.json').write_text(json.dumps({'hypothesis':'same-event patterns remain underfit in broad-history objective: latest2023StLite4/19 in-sample winners with19/5865 teacher races','control':'priority-race-context-restoration-001','intervention':'one fixed50:50 unique-race group-weight mass split, not a claim of equal realized loss gradients','all_teacher_rows_retained_positive':True,'same168features_and_source':True,'iterations':500,'maximum_fits':8,'years':[2020,2021,2022,2023],'no_weight_grid':True,'later_data_not_used_for_selection':True,'drivers':drivers,'production_eligible':False},indent=2),encoding='utf-8')
print('EVENT_WEIGHT_INTERVENTION_FROZEN',flush=True)
PY
for year in 2020 2021 2022 2023; do
  bash "research/jra-20260913/run-event-weight-${year}-001.sh" > "research/jra-20260913/logs/event-weight-${year}-001.log" 2>&1
  printf '0\n' > "research/jra-20260913/logs/event-weight-${year}-001.exit"
  printf 'EVENT_WEIGHT_YEAR_COMPLETE %s\n' "$year"
done
