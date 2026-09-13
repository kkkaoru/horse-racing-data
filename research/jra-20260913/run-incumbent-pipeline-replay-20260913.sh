#!/usr/bin/env bash
# Offline production routing/postprocessing replay; never flush predictions.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export PYTHONPATH="$PWD/apps/finish-position-predict-container/src:${PYTHONPATH:-}"
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from functools import partialmethod
from pathlib import Path
from unittest.mock import patch

from catboost import CatBoost
import predict_upcoming as production
from predict_lib.model_meta import Architecture
from predict_lib.upsert_sql import INSERT_COLUMNS

root=Path('research/jra-20260913')
out=root/'incumbent-pipeline-replay-20260913-001'
out.mkdir(exist_ok=False)
original=json.loads((root/'cloudflare-003/predictions-20260913.json').read_text(encoding='utf-8'))
races={}
for race in original['races']:
    path=root/'live-features-002'/f"20260913-{race['keibajoCode']}-{int(race['raceNumber']):02d}.parquet"
    loaded=production._load_cached_races(path)
    if set(loaded)!={race['raceId']}:
        raise ValueError('Cached race identity changed')
    if sorted(int(x['umaban']) for x in loaded[race['raceId']])!=sorted(int(x['horseNumber']) for x in race['prediction']):
        raise ValueError('Declared roster changed; no silent removal allowed')
    races.update(loaded)
real_matrix_builder=production.build_feature_matrix
calls=[]

def checked_matrix(entries: Sequence[Mapping[str, object]], names: Sequence[str], architecture: Architecture) -> list[list[float]]:
    missing=sorted(set(names)-set(entries[0]))
    if missing:
        (out/'missing-inputs.json').write_text(json.dumps(missing,indent=2),encoding='utf-8')
        raise ValueError('Runtime requested absent features; refusing zero fill')
    calls.append({'race_id':str(entries[0].get('race_id')),'feature_count':len(names),'feature_names_sha256':hashlib.sha256(json.dumps(list(names)).encode()).hexdigest()})
    return real_matrix_builder(entries,names,architecture)

# Only change native prediction thread count; model and ranking logic remain unchanged.
with patch.object(CatBoost,'predict',partialmethod(CatBoost.predict,thread_count=4)), patch.object(production,'build_feature_matrix',checked_matrix):
    scored=production.score_races(races,'jra',Path('apps/finish-position-predict-container/models'),card_max_race_bango=12,race_names_by_race_id={r['raceId']:{'kyosomei_hondai':r['raceName']} for r in original['races']})
by_race={}
for batch in scored:
    records=[dict(zip(INSERT_COLUMNS,row,strict=True)) for row in batch]
    if not records:
        continue
    first=records[0]
    race_id=':'.join(str(first[name]) for name in ('source','kaisai_nen','kaisai_tsukihi','keibajo_code','race_bango'))
    by_race[race_id]=records
results=[]
for race in original['races']:
    records=by_race.get(race['raceId'],[])
    served=[int(x['horseNumber']) for x in sorted(race['prediction'],key=lambda x:x['rank'])]
    order=[int(x['umaban']) for x in sorted(records,key=lambda x:x['predicted_rank'])]
    versions=sorted({str(x['model_version']) for x in records})
    results.append({'race_id':race['raceId'],'served_version':race['modelVersion'],'runtime_versions':versions,'version_match':versions==[race['modelVersion']],'served_order':served,'runtime_order':order,'all_ranks_match':served==order,'top5_match':served[:5]==order[:5],'roster_equal':sorted(served)==sorted(order)})
report={'races':results,'matrix_calls':calls,'all_rank_matches':sum(r['all_ranks_match'] for r in results),'version_matches':sum(r['version_match'] for r in results),'runtime_flag':{'STAGE1_PRESERVED_ODDS_GATE_ENABLED':os.environ.get('STAGE1_PRESERVED_ODDS_GATE_ENABLED'),'JRA_ETOP2_ENABLED':production.JRA_ETOP2_ENABLED,'JRA_DIRT_HYBRID_ENABLED':production.JRA_DIRT_HYBRID_ENABLED},'limitations':['current local routing/artifact bytes, not independently proven original served bytes','R2 upload9-14seconds after prediction timestamp may reflect producer order; timestamp alone does not establish feature leakage','no refreshed or substituted odds, no labels used in scoring','no production writes, no adoption evidence']}
(out/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print('INCUMBENT_PIPELINE_REPLAY_COMPLETE',report['all_rank_matches'],report['version_matches'],len(results),flush=True)
PY
