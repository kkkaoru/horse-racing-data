#!/usr/bin/env bash
# Reuse verified production context formulas, preserving the 500-tree controls.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import ast
import hashlib
import json
from pathlib import Path

root=Path('research/jra-20260913')
out=root/'priority-context-001'
out.mkdir(exist_ok=False)
archive=out/'source-code'
archive.mkdir()
spec=json.loads((root/'context-availability-001/report.json').read_text(encoding='utf-8'))
if not spec['all_equal'] or spec['rows']!=314:
    raise ValueError('Live canonical-context parity must pass first')
fragment="""
context_input = frame.copy(deep=False)
pieces = context_input['race_id'].str.split(':',expand=True)
for column, position in [('source',0),('kaisai_nen',1),('kaisai_tsukihi',2),('keibajo_code',3),('race_bango',4)]:
    context_input[column] = pieces[position]
context_input['ketto_toroku_bango'] = context_input['horse_id']
context_input['umaban'] = context_input['horse_number']
canonical_sql = (ROOT/'context-availability-001/canonical-query.sql').read_text(encoding='utf-8')
context_names = list(CONTEXT_SPEC['features'])
with duckdb.connect() as context_connection:
    context_connection.execute('SET threads=4')
    context_connection.execute("SET memory_limit='2GB'")
    context_connection.execute(f"SET temp_directory='{OUT.as_posix()}/duckdb-scratch'")
    context_connection.register('canonical_input',context_input.loc[:,CONTEXT_SPEC['base_columns']])
    derived = context_connection.execute('SELECT race_id,ketto_toroku_bango,umaban,'+','.join(context_names)+' FROM ('+canonical_sql+')').fetchdf().rename(columns={'ketto_toroku_bango':'horse_id','umaban':'horse_number'})
if len(derived)!=len(frame):
    raise ValueError('Context calculation changed source roster')
frame = frame.merge(derived,on=identity,how='left',validate='one_to_one').sort_values(identity)
del context_input,pieces,derived
print('CANONICAL_CONTEXT_ADDED',len(context_names),'rows',len(frame),flush=True)
"""
drivers=[]
for year in (2020,2021,2022,2023):
    name=f'run-context-marketfree-{year}-001.sh'
    script=(root/f'run-priority-marketfree-{year}-001.sh').read_text(encoding='utf-8')
    script=script.replace(f'priority-capacity-001/marketfree/year-{year}',f'priority-context-001/marketfree/year-{year}')
    script=script.replace('import numpy as np','import duckdb\nimport numpy as np')
    line="FEATURES = market_free_features(json.loads(SCHEMA_PATH.read_text(encoding='utf-8'))['numeric_feature_names'])"
    if script.count(line)!=1:
        raise ValueError('Unexpected baseline feature contract')
    script=script.replace(line,"CONTEXT_SPEC = json.loads((ROOT/'context-availability-001/report.json').read_text(encoding='utf-8'))\nBASE_FEATURES = market_free_features(json.loads(SCHEMA_PATH.read_text(encoding='utf-8'))['numeric_feature_names'])\nFEATURES = (*BASE_FEATURES,*CONTEXT_SPEC['features'])")
    script=script.replace("'tansho_odds',*FEATURES]","'tansho_odds',*BASE_FEATURES]")
    script=script.replace("print('COMPLETE_SOURCE_ROSTER_LOADED'",fragment+"\nprint('COMPLETE_SOURCE_ROSTER_LOADED'")
    script=script.replace("'market_used_in_training':False","'context_formula_source_sha256':CONTEXT_SPEC['source_sha256'],'live_context_parity_rows':314,'market_used_in_training':False")
    marker="(OUT/'cohorts.json').write_text"
    check=f"baseline = json.loads((ROOT/'priority-capacity-001/marketfree/year-{year}/cohorts.json').read_text(encoding='utf-8'))\nfor current, previous in zip(CASES,baseline,strict=True):\n    if current['cell_id'] != previous['cell_id'] or tuple(current['training_race_ids']) != tuple(previous['training_race_ids']) or tuple(current['evaluation_race_ids']) != tuple(previous['evaluation_race_ids']):\n        raise ValueError('Cohort drift from capacity control')\n"
    script=script.replace(marker,check+marker)
    ast.parse(script.split("<<'PY'\n",1)[1].rsplit('\nPY',1)[0])
    with (root/name).open('x',encoding='utf-8') as stream:
        stream.write(script)
    (archive/name).write_text(script,encoding='utf-8')
    drivers.append({'year':year,'path':name,'sha256':hashlib.sha256(script.encode()).hexdigest()})
for name in ('fold_ranker.py','dedicated_cells.py','race_rank_review.py'):
    (archive/name).write_bytes((root/name).read_bytes())
(archive/'canonical-query.sql').write_bytes((root/'context-availability-001/canonical-query.sql').read_bytes())
(out/'freeze.json').write_text(json.dumps({'intervention':'twenty canonical race context features only','controls':'priority-capacity-001/marketfree','features':spec['features'],'live_semantic_parity':{'races':24,'rows':314,'all_twenty_equal':True,'rtol':1e-6,'atol':1e-6},'iterations':500,'recency_weighting':False,'maximum_fits':8,'years':[2020,2021,2022,2023],'drivers':drivers,'missingness':'original raw inputs remain NaN; derived sums follow existing canonical SQL COALESCE semantics; no outcome or roster imputation','production_eligible':False},indent=2),encoding='utf-8')
print('CONTEXT_PRIORITY_FROZEN',flush=True)
PY
for year in 2020 2021 2022 2023; do
  bash "research/jra-20260913/run-context-marketfree-${year}-001.sh" > "research/jra-20260913/logs/context-marketfree-${year}-001.log" 2>&1
  printf '0\n' > "research/jra-20260913/logs/context-marketfree-${year}-001.exit"
  printf 'CONTEXT_YEAR_COMPLETE %s\n' "$year"
done
