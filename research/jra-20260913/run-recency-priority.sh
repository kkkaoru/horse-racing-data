#!/usr/bin/env bash
# Exactly one intervention against the retained 500-tree priority controls.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
bash research/chronos2/run-local.sh "$ROOT/apps/timesfm-finish-position/.venv/bin/python" - <<'PY'
import ast
import hashlib
import json
from pathlib import Path

root=Path('research/jra-20260913')
out=root/'priority-recency-001'
out.mkdir(exist_ok=False)
archive=out/'source-code'
archive.mkdir()
drivers=[]
for year in (2020,2021,2022,2023):
    old=f'run-priority-marketfree-{year}-001.sh'
    name=f'run-recency-marketfree-{year}-001.sh'
    script=(root/old).read_text(encoding='utf-8')
    script=script.replace(f"priority-capacity-001/marketfree/year-{year}",f"priority-recency-001/marketfree/year-{year}")
    script=script.replace('from race_rank_review import review_orders','from race_rank_review import review_orders\nfrom recency_weights import RecencyConfig, chronological_weights')
    script=script.replace("'market_used_in_training':False", "'recency_half_life_days':1826.25,'race_weight_mean':1,'all_teacher_weights_positive':True,'market_used_in_training':False")
    script=script.replace("    print('FIT_START'", "    recency_config = RecencyConfig(history_start=date.fromisoformat(str(case['history_start'])), cutoff=date.fromisoformat(str(case['cutoff'])))\n    group_weights = chronological_weights(race_ids=np.asarray(train['race_id'],dtype=np.str_), dates={race.race_id:race.race_date for race in RACES}, config=recency_config)\n    print('FIT_START'")
    script=script.replace('output=target,config=CONFIG)', 'output=target,config=CONFIG,group_weights=group_weights)')
    script=script.replace('    reloaded = CatBoostRanker()', "    np.save(target/'group-weights.npy',group_weights,allow_pickle=False)\n    (target/'recency.json').write_text(json.dumps({'half_life_days':recency_config.half_life_days,'minimum_weight':float(group_weights.min()),'maximum_weight':float(group_weights.max()),'rows':len(group_weights),'no_dropped_teachers':True},indent=2),encoding='utf-8')\n    reloaded = CatBoostRanker()")
    marker="(OUT/'cohorts.json').write_text"
    check=f"baseline = json.loads((ROOT/'priority-capacity-001/marketfree/year-{year}/cohorts.json').read_text(encoding='utf-8'))\nfor current, previous in zip(CASES,baseline,strict=True):\n    if current['cell_id'] != previous['cell_id'] or tuple(current['training_race_ids']) != tuple(previous['training_race_ids']) or tuple(current['evaluation_race_ids']) != tuple(previous['evaluation_race_ids']):\n        raise ValueError('Cohort drift from fixed capacity control')\n"
    script=script.replace(marker,check+marker)
    ast.parse(script.split("<<'PY'\n",1)[1].rsplit('\nPY',1)[0])
    with (root/name).open('x',encoding='utf-8') as stream:
        stream.write(script)
    (archive/name).write_text(script,encoding='utf-8')
    drivers.append({'year':year,'path':name,'sha256':hashlib.sha256(script.encode()).hexdigest()})
for name in ('fold_ranker.py','recency_weights.py','dedicated_cells.py','race_rank_review.py'):
    (archive/name).write_bytes((root/name).read_bytes())
(out/'freeze.json').write_text(json.dumps({'intervention':'five-year positive race-weight decay only','controls':'priority-capacity-001/marketfree','iterations':500,'maximum_fits':8,'years':[2020,2021,2022,2023],'drivers':drivers,'production_eligible':False},indent=2),encoding='utf-8')
print('RECENCY_PRIORITY_FROZEN',flush=True)
PY
for year in 2020 2021 2022 2023; do
  bash "research/jra-20260913/run-recency-marketfree-${year}-001.sh" > "research/jra-20260913/logs/recency-marketfree-${year}-001.log" 2>&1
  printf '0\n' > "research/jra-20260913/logs/recency-marketfree-${year}-001.exit"
  printf 'RECENCY_YEAR_COMPLETE %s\n' "$year"
done
