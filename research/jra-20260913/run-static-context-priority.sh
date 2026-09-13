#!/usr/bin/env bash
# Add explicit condition descriptors to the unchanged mixed-source117 control.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import ast
import hashlib
import json
from pathlib import Path

root=Path('research/jra-20260913')
out=root/'priority-static-context-001'
out.mkdir(exist_ok=False)
archive=out/'source-code'
archive.mkdir()
fragment="""
context_rows = [{'race_id':race.race_id,'keibajo_code':numeric_race_code(race.venue),'track_code':numeric_race_code(race.track_code),'kyoso_joken_code':numeric_race_code(race.condition_code)} for race in RACES]
with gzip.open(ROOT/'nvd-full-roster-001/races.json.gz','rt',encoding='utf-8') as stream:
    nvd_metadata = json.load(stream)
for race in nvd_metadata:
    race_id='nar:'+':'.join(race[key] for key in ('kaisai_nen','kaisai_tsukihi','keibajo_code','race_bango'))
    context_rows.append({'race_id':race_id,**{key:numeric_race_code(race[key]) for key in STATIC_CONTEXT_NAMES}})
context = pd.DataFrame(context_rows).set_index('race_id',verify_integrity=True)
for key in STATIC_CONTEXT_NAMES:
    frame[key] = frame['race_id'].map(context[key])
(OUT/'static-context-codebook.json').write_text(json.dumps({'names':STATIC_CONTEXT_NAMES,'alphabetic_code_offset':1000,'live_jra_codes':'decimal strings remain identical to scorer float conversion','foreign_codes':'separate categorical numeric identifiers, not a physical scale'},indent=2),encoding='utf-8')
print('STATIC_CONTEXT_ATTACHED',len(STATIC_CONTEXT_NAMES),flush=True)
"""
entries=[]
for year in (2020,2021,2022,2023):
    name=f'run-static-context-{year}-001.sh'
    script=(root/f'run-common-source-jvd-plus-nvd-{year}-001.sh').read_text(encoding='utf-8')
    script=script.replace(f'priority-common-source-001/jvd-plus-nvd/year-{year}',f'priority-static-context-001/year-{year}')
    script=script.replace('import json','import gzip\nimport json',1)
    script=script.replace('from race_rank_review import review_orders','from race_rank_review import review_orders\nfrom static_race_context import STATIC_CONTEXT_NAMES, numeric_race_code')
    script=script.replace("FEATURES = market_free_features(COMMON['feature_names'])","BASE_FEATURES = market_free_features(COMMON['feature_names'])\nFEATURES = (*BASE_FEATURES,*STATIC_CONTEXT_NAMES)")
    script=script.replace("'tansho_odds',*FEATURES]","'tansho_odds',*BASE_FEATURES]")
    script=script.replace("print('COMPLETE_SOURCE_ROSTER_LOADED'",fragment+"\nprint('COMPLETE_SOURCE_ROSTER_LOADED'")
    script=script.replace("'common_schema_count':117","'shared_history_feature_count':117,'explicit_condition_features':3,'input_count':120")
    script=script.replace("'source_scope':'JVD; NVD cross-source completion remains pending'","'source_scope':'frozen JVD+NVD with explicit source availability limits'")
    ast.parse(script.split("<<'PY'\n",1)[1].rsplit('\nPY',1)[0])
    with (root/name).open('x',encoding='utf-8') as stream:
        stream.write(script)
    (archive/name).write_text(script,encoding='utf-8')
    entries.append({'year':year,'path':name,'sha256':hashlib.sha256(script.encode()).hexdigest()})
for name in ('fold_ranker.py','dedicated_cells.py','race_rank_review.py','static_race_context.py'):
    (archive/name).write_bytes((root/name).read_bytes())
(out/'freeze.json').write_text(json.dumps({'hypothesis':'broad cross-venue/surface/class teachers need explicit current race-condition identifiers, which are absent from117/168 numeric-only feature selections','control':'priority-common-source-001/jvd-plus-nvd','input_count':120,'unchanged_source_and_labels':True,'parameters':'500 YetiRank, no weight or objective change','years':[2020,2021,2022,2023],'maximum_fits':8,'source_completion_limits_unchanged':True,'drivers':entries,'production_eligible':False},indent=2),encoding='utf-8')
print('STATIC_CONTEXT_COMPARISON_FROZEN',flush=True)
PY
for year in 2020 2021 2022 2023; do
  bash "research/jra-20260913/run-static-context-${year}-001.sh" > "research/jra-20260913/logs/static-context-${year}-001.log" 2>&1
  printf '0\n' > "research/jra-20260913/logs/static-context-${year}-001.exit"
  printf 'STATIC_CONTEXT_YEAR_COMPLETE %s\n' "$year"
done
