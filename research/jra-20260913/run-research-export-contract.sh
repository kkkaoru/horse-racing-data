#!/usr/bin/env bash
# Local-only export contract probe using failed development models, not promotion.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export PYTHONPATH="$PWD/apps/finish-position-predict-container/src"
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from catboost import CatBoostRanker
from catboost_adapter import load_catboost_booster
from predict_lib.scorer import build_feature_matrix, score_matrix

root=Path('research/jra-20260913')
out=root/'research-export-contract-001'
out.mkdir(exist_ok=False)
base=root/'priority-context-001/marketfree/year-2023'
protocol=json.loads((base/'protocol.json').read_text(encoding='utf-8'))
original_names=protocol['feature_names']
serving_names=['shusso_tosu_1' if name=='shusso_tosu' else name for name in original_names]
if len(set(serving_names))!=len(serving_names):
    raise ValueError('Export mapping introduced duplicate input names')
lineage=Path('apps/pc-keiba-viewer/src/scripts/finish-position-features/add-near-miss-features.py')
manifest={'promotion_eligible':False,'purpose':'runtime contract only; both chosen development models failed selection','mapping':{'shusso_tosu':'shusso_tosu_1'},'reason':'near-miss layer intentionally emits constant-NULL unsuffixed legacy column; real field size survives in suffixed column','lineage_path':str(lineage),'lineage_sha256':hashlib.sha256(lineage.read_bytes()).hexdigest(),'feature_names':serving_names,'results':[]}
for cell in ('jra-event-nakayama-st-lite-2200-v1','jra-event-hanshin-challenge-2000-v1'):
    source=base/cell/'year-2023/model.cbm'
    target=out/cell
    target.mkdir()
    model=CatBoostRanker()
    model.load_model(str(source))
    model.set_feature_names(serving_names)
    model.save_model(str(target/'model.json'),format='json')
    booster=load_catboost_booster(str(target/'model.json'))
    rows=[]
    for path in sorted((root/'live-features-002').glob('*.parquet')):
        frame=pd.read_parquet(path)
        expected=frame.copy(deep=False)
        expected['shusso_tosu']=frame['shusso_tosu_1']
        reference_matrix=np.asarray(expected.loc[:,original_names],dtype=np.float32)
        runtime_matrix=build_feature_matrix(frame.to_dict(orient='records'),serving_names,'catboost')
        matrix_equal=np.array_equal(reference_matrix,np.asarray(runtime_matrix,dtype=np.float32),equal_nan=True)
        native=np.asarray(model.predict(reference_matrix,thread_count=4),dtype=np.float64)
        served=np.asarray(score_matrix(booster,runtime_matrix),dtype=np.float64)
        same_order=np.array_equal(np.argsort(-native,kind='stable'),np.argsort(-served,kind='stable'))
        error=float(np.max(np.abs(native-served)))
        rows.append({'race_file':path.name,'rows':len(frame),'float32_matrix_equal':bool(matrix_equal),'max_score_error':error,'order_equal':bool(same_order)})
    result={'cell':cell,'source_model_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'checks':rows,'passed':all(row['float32_matrix_equal'] and row['order_equal'] and row['max_score_error']<1e-6 for row in rows)}
    manifest['results'].append(result)
    print('RESEARCH_EXPORT_CONTRACT',cell,result['passed'],flush=True)
(out/'report.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
PY
