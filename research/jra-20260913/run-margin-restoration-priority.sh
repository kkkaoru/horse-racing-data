#!/usr/bin/env bash
# Fixed comparison: restore missing NAR margins and their dependent input ranks.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import ast
import hashlib
import json
from pathlib import Path

root=Path('research/jra-20260913')
source=root/'nvd-margin-restoration-001'
manifest=json.loads((source/'manifest.json').read_text(encoding='utf-8'))
if manifest['rows']!=12331 or not manifest['other_columns_exactly_preserved']:
    raise ValueError('Restored teacher identities or untouched values changed')
out=root/'priority-margin-restoration-001'
out.mkdir(exist_ok=False)
archive=out/'source-code'
archive.mkdir()
with (source/'nvd-teachers.parquet').open('rb') as stream:
    digest=hashlib.file_digest(stream,'sha256').hexdigest()
drivers=[]
for year in (2020,2021,2022,2023):
    name=f'run-margin-restoration-{year}-001.sh'
    script=(root/f'run-common-source-jvd-plus-nvd-{year}-001.sh').read_text(encoding='utf-8')
    old="NVD = pd.read_parquet(ROOT/'common-schema-source-001/nvd-teachers.parquet')"
    if script.count(old)!=1:
        raise ValueError('Unexpected parent source declaration')
    script=script.replace(old,"NVD = pd.read_parquet(ROOT/'nvd-margin-restoration-001/nvd-teachers.parquet')")
    script=script.replace(f'priority-common-source-001/jvd-plus-nvd/year-{year}',f'priority-margin-restoration-001/year-{year}')
    script=script.replace("'market_used_in_training':False",f"'restored_nvd_sha256':'{digest}','changed_nvd_columns':{manifest['changed_columns']!r},'market_used_in_training':False")
    script=script.replace("'source_scope':'JVD; NVD cross-source completion remains pending'","'source_scope':'JVD+NVD, explicit margin restoration; other source limits remain'")
    ast.parse(script.split("<<'PY'\n",1)[1].rsplit('\nPY',1)[0])
    with (root/name).open('x',encoding='utf-8') as stream:
        stream.write(script)
    (archive/name).write_text(script,encoding='utf-8')
    drivers.append({'year':year,'path':name,'sha256':hashlib.sha256(script.encode()).hexdigest()})
for name in ('fold_ranker.py','dedicated_cells.py','race_rank_review.py','margin_features.py','MARGIN_RESTORATION_PLAN.md','run-nvd-margin-restoration.sh'):
    (archive/name).write_bytes((root/name).read_bytes())
(out/'freeze.json').write_text(json.dumps({'comparison':'same JVD+NVD117 inputs, restore6 entirely missing columns and recompute2 dependent ranks','control':'priority-common-source-001/jvd-plus-nvd','iterations':500,'maximum_fits':8,'years':[2020,2021,2022,2023],'weights':'unchanged; no recency','static_descriptors_added':False,'nvd_input_sha256':digest,'source_restoration':manifest,'drivers':drivers,'production_eligible':False},indent=2),encoding='utf-8')
print('MARGIN_RESTORATION_COMPARISON_FROZEN',flush=True)
PY
for year in 2020 2021 2022 2023; do
  bash "research/jra-20260913/run-margin-restoration-${year}-001.sh" > "research/jra-20260913/logs/margin-restoration-${year}-001.log" 2>&1
  printf '0\n' > "research/jra-20260913/logs/margin-restoration-${year}-001.exit"
  printf 'MARGIN_RESTORATION_YEAR_COMPLETE %s\n' "$year"
done
