#!/usr/bin/env bash
# Fixed source-population ablation with identical common inputs in both arms.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import ast
import hashlib
import json
from pathlib import Path

root=Path('research/jra-20260913')
out=root/'priority-common-source-001'
out.mkdir(exist_ok=False)
archive=out/'source-code'
archive.mkdir()
manifest=json.loads((root/'common-schema-source-001/manifest.json').read_text(encoding='utf-8'))
if len(manifest['feature_names'])!=117 or manifest['rows']!=12331:
    raise ValueError('Unexpected common-source staging contract')
drivers=[]
for year in (2020,2021,2022,2023):
    for arm in ('jvd-only','jvd-plus-nvd'):
        name=f'run-common-source-{arm}-{year}-001.sh'
        script=(root/f'run-priority-marketfree-{year}-001.sh').read_text(encoding='utf-8')
        script=script.replace(f'priority-capacity-001/marketfree/year-{year}',f'priority-common-source-001/{arm}/year-{year}')
        old="FEATURES = market_free_features(json.loads(SCHEMA_PATH.read_text(encoding='utf-8'))['numeric_feature_names'])"
        if script.count(old)!=1:
            raise ValueError('Unexpected parent schema declaration')
        script=script.replace(old,"COMMON = json.loads((ROOT/'common-schema-source-001/manifest.json').read_text(encoding='utf-8'))\nFEATURES = market_free_features(COMMON['feature_names'])")
        script=script.replace("'market_used_in_training':False",f"'source_arm':'{arm}','common_schema_count':117,'not_full_168_feature_restoration':True,'market_used_in_training':False")
        marker="(OUT/'cohorts.json').write_text"
        verification=f"baseline = json.loads((ROOT/'priority-capacity-001/marketfree/year-{year}/cohorts.json').read_text(encoding='utf-8'))\nfor current, previous in zip(CASES,baseline,strict=True):\n    for key in ('cell_id','seed_horse_ids','training_race_ids','evaluation_race_ids'):\n        if json.dumps(current[key],default=str)!=json.dumps(previous[key],default=str):\n            raise ValueError('Cohort drift before source intervention')\n"
        augmentation=""
        if arm=='jvd-plus-nvd':
            augmentation="""
NVD = pd.read_parquet(ROOT/'common-schema-source-001/nvd-teachers.parquet')
NVD_CASES = json.loads((ROOT/'nvd-seed-coverage-001/report.json').read_text(encoding='utf-8'))['results']
NVD_ALLOWED = set(NVD['race_id'])
for case in CASES:
    matching = next(item for item in NVD_CASES if item['year']==case['evaluation_year'] and item['cell']==case['cell_id'])
    additional = set(matching['new_race_ids']) & NVD_ALLOWED
    if {rid.replace('nar:','jra:',1) for rid in additional} & set(case['training_race_ids']):
        raise ValueError('Duplicate physical race across source namespaces')
    case['jvd_training_race_ids'] = case['training_race_ids']
    case['nvd_training_race_ids'] = tuple(sorted(additional))
    case['training_race_ids'] = tuple(sorted(set(case['training_race_ids']) | additional))
"""
            script=script.replace("print('COMPLETE_SOURCE_ROSTER_LOADED'","frame = pd.concat([frame,NVD],ignore_index=True,sort=False).sort_values(identity)\nif frame.duplicated(identity).any():\n    raise ValueError('Duplicate source runner identities after augmentation')\nprint('COMPLETE_SOURCE_ROSTER_LOADED'")
        script=script.replace(marker,verification+augmentation+marker)
        ast.parse(script.split("<<'PY'\n",1)[1].rsplit('\nPY',1)[0])
        with (root/name).open('x',encoding='utf-8') as stream:
            stream.write(script)
        (archive/name).write_text(script,encoding='utf-8')
        drivers.append({'year':year,'arm':arm,'path':name,'sha256':hashlib.sha256(script.encode()).hexdigest()})
for name in ('fold_ranker.py','dedicated_cells.py','race_rank_review.py'):
    (archive/name).write_bytes((root/name).read_bytes())
(out/'freeze.json').write_text(json.dumps({'comparison':'identical 117 inputs; JVD-only versus JVD+NVD','no_168_feature_restoration_claim':True,'iterations':500,'maximum_fits':16,'years':[2020,2021,2022,2023],'weights':'unchanged, no recency','drivers':drivers,'nvd_input_sha256':hashlib.sha256((root/'common-schema-source-001/nvd-teachers.parquet').read_bytes()).hexdigest(),'official_label_overlay':manifest['official_label_overlay'],'noneligible_races_explicitly_ledgered':True,'missing_rows_retained_as_nan':True,'production_eligible':False},indent=2),encoding='utf-8')
print('COMMON_SOURCE_COMPARISON_FROZEN',flush=True)
PY
for year in 2020 2021 2022 2023; do
  for arm in jvd-only jvd-plus-nvd; do
    bash "research/jra-20260913/run-common-source-${arm}-${year}-001.sh" > "research/jra-20260913/logs/common-source-${arm}-${year}-001.log" 2>&1
    printf '0\n' > "research/jra-20260913/logs/common-source-${arm}-${year}-001.exit"
    printf 'COMMON_SOURCE_YEAR_COMPLETE %s %s\n' "$year" "$arm"
  done
done
