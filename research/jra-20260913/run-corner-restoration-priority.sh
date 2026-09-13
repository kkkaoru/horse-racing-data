#!/usr/bin/env bash
# Matched168-input comparison isolates24 restored NVD corner columns.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import ast
import hashlib
import json
from pathlib import Path

root=Path('research/jra-20260913')
source=root/'nvd-corner-restoration-001'
manifest=json.loads((source/'manifest.json').read_text(encoding='utf-8'))
if manifest['rows']!=12331 or len(manifest['new_corner_columns'])!=24 or not manifest['shared_columns_exactly_unchanged']:
    raise ValueError('Corner restoration contract failed')
out=root/'priority-corner-restoration-001'
out.mkdir(exist_ok=False)
archive=out/'source-code'
archive.mkdir()
drivers=[]
inputs={}
for arm in ('control','restored'):
    path=source/f'{arm}-teachers.parquet'
    with path.open('rb') as stream:
        inputs[arm]=hashlib.file_digest(stream,'sha256').hexdigest()
for year in (2020,2021,2022,2023):
    for arm in ('control','restored'):
        name=f'run-corner-restoration-{arm}-{year}-001.sh'
        script=(root/f'run-common-source-jvd-plus-nvd-{year}-001.sh').read_text(encoding='utf-8')
        old="NVD = pd.read_parquet(ROOT/'common-schema-source-001/nvd-teachers.parquet')"
        if script.count(old)!=1:
            raise ValueError('Unexpected parent NVD loading declaration')
        script=script.replace(old,f"NVD = pd.read_parquet(ROOT/'nvd-corner-restoration-001/{arm}-teachers.parquet')")
        script=script.replace("FEATURES = market_free_features(COMMON['feature_names'])","FEATURES = market_free_features(json.loads(SCHEMA_PATH.read_text(encoding='utf-8'))['numeric_feature_names'])\nif len(FEATURES)!=168:\n    raise ValueError('Expected168 original market-free inputs')")
        script=script.replace(f'priority-common-source-001/jvd-plus-nvd/year-{year}',f'priority-corner-restoration-001/{arm}/year-{year}')
        script=script.replace("'common_schema_count':117",f"'input_count':168,'corner_arm':'{arm}','nvd_input_sha256':'{inputs[arm]}'")
        script=script.replace("'source_scope':'JVD; NVD cross-source completion remains pending'","'source_scope':'JVD+NVD with explicitly incomplete NVD168 inputs'")
        ast.parse(script.split("<<'PY'\n",1)[1].rsplit('\nPY',1)[0])
        with (root/name).open('x',encoding='utf-8') as stream:
            stream.write(script)
        (archive/name).write_text(script,encoding='utf-8')
        drivers.append({'year':year,'arm':arm,'path':name,'sha256':hashlib.sha256(script.encode()).hexdigest()})
for name in ('fold_ranker.py','dedicated_cells.py','race_rank_review.py','run-nvd-corner-restoration.sh'):
    (archive/name).write_bytes((root/name).read_bytes())
(out/'freeze.json').write_text(json.dumps({'comparison':'same168 features and mixed population; control has51 absent NVD inputs asNaN, treatment restores24corner inputs only','margin_restoration_present_in_both':True,'maximum_fits':16,'years':[2020,2021,2022,2023],'iterations':500,'weights':'unchanged','remaining_absent_columns':27,'full168_restoration_claim':False,'inputs':inputs,'source_manifest':manifest,'drivers':drivers,'production_eligible':False},indent=2),encoding='utf-8')
print('CORNER_RESTORATION_COMPARISON_FROZEN',flush=True)
PY
for year in 2020 2021 2022 2023; do
  for arm in control restored; do
    bash "research/jra-20260913/run-corner-restoration-${arm}-${year}-001.sh" > "research/jra-20260913/logs/corner-restoration-${arm}-${year}-001.log" 2>&1
    printf '0\n' > "research/jra-20260913/logs/corner-restoration-${arm}-${year}-001.exit"
    printf 'CORNER_RESTORATION_YEAR_COMPLETE %s %s\n' "$year" "$arm"
  done
done
