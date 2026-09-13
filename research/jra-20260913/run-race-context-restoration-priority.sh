#!/usr/bin/env bash
# Fixed168-input comparison: seven formerly absent NVD age/career context inputs.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import ast
import hashlib
import json
from pathlib import Path

root=Path('research/jra-20260913')
source=root/'nvd-race-context-restoration-003'
manifest=json.loads((source/'manifest.json').read_text(encoding='utf-8'))
if manifest['rows']!=12331 or len(manifest['changed_columns'])!=7 or not manifest['other_columns_exactly_preserved']:
    raise ValueError('Race context restoration contract failed')
out=root/'priority-race-context-restoration-001'
out.mkdir(exist_ok=False)
archive=out/'source-code'
archive.mkdir()
with (source/'nvd-teachers.parquet').open('rb') as stream:
    digest=hashlib.file_digest(stream,'sha256').hexdigest()
old_digest=json.loads((root/'priority-corner-restoration-001/freeze.json').read_text(encoding='utf-8'))['inputs']['restored']
drivers=[]
for year in (2020,2021,2022,2023):
    name=f'run-race-context-restoration-{year}-001.sh'
    script=(root/f'run-corner-restoration-restored-{year}-001.sh').read_text(encoding='utf-8')
    old="NVD = pd.read_parquet(ROOT/'nvd-corner-restoration-001/restored-teachers.parquet')"
    if script.count(old)!=1:
        raise ValueError('Unexpected parent NVD input')
    script=script.replace(old,"NVD = pd.read_parquet(ROOT/'nvd-race-context-restoration-003/nvd-teachers.parquet')")
    script=script.replace(f'priority-corner-restoration-001/restored/year-{year}',f'priority-race-context-restoration-001/year-{year}')
    script=script.replace(old_digest,digest)
    script=script.replace("'market_used_in_training':False","'race_context_restored':True,'market_used_in_training':False")
    ast.parse(script.split("<<'PY'\n",1)[1].rsplit('\nPY',1)[0])
    with (root/name).open('x',encoding='utf-8') as stream:
        stream.write(script)
    (archive/name).write_text(script,encoding='utf-8')
    drivers.append({'year':year,'path':name,'sha256':hashlib.sha256(script.encode()).hexdigest()})
for name in ('fold_ranker.py','dedicated_cells.py','race_rank_review.py','run-nvd-race-context-restoration-003.sh'):
    (archive/name).write_bytes((root/name).read_bytes())
(out/'freeze.json').write_text(json.dumps({'comparison':'same168 inputs and mixed population; restore seven NVD age/career context values only','control':'priority-corner-restoration-001/restored','maximum_fits':8,'iterations':500,'years':[2020,2021,2022,2023],'weights':'unchanged','remaining_absent_columns':20,'nvd_input_sha256':digest,'restoration_manifest':manifest,'drivers':drivers,'production_eligible':False},indent=2),encoding='utf-8')
print('RACE_CONTEXT_RESTORATION_FROZEN',flush=True)
PY
for year in 2020 2021 2022 2023; do
  bash "research/jra-20260913/run-race-context-restoration-${year}-001.sh" > "research/jra-20260913/logs/race-context-restoration-${year}-001.log" 2>&1
  printf '0\n' > "research/jra-20260913/logs/race-context-restoration-${year}-001.exit"
  printf 'RACE_CONTEXT_RESTORATION_YEAR_COMPLETE %s\n' "$year"
done
