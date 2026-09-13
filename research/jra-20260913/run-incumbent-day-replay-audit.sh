#!/usr/bin/env bash
# Read-only schema/timing/rank replay of the actually served 9/12 model families.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export PYTHONPATH="$PWD/apps/finish-position-predict-container/src:${PYTHONPATH:-}"
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import hashlib
import json
import os
import urllib.request
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
from catboost import CatBoost
from predict_lib.r2_client import _build_signed_request
from predict_lib.scorer import build_feature_matrix
from predict_lib.serve import R2Config
from predict_lib.upcoming import rank_race_entries
from predict_upcoming import _load_cached_races

root=Path('research/jra-20260913')
out=root/'incumbent-day-replay-001'
out.mkdir(exist_ok=False)
original=json.loads((root/'cloudflare-002/predictions-20260912.json').read_text(encoding='utf-8'))
model_root=Path('apps/finish-position-predict-container/models/finish-position/jra')
config=R2Config(account_id=os.environ['R2_ACCOUNT_ID'],access_key_id=os.environ['R2_ACCESS_KEY_ID'],secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'],bucket='pc-keiba-features-archive')
models={}
rows=[]
for race in original['races']:
    stem=f"20260912-{race['keibajoCode']}-{int(race['raceNumber']):02d}"
    parquet=root/'historical-features-20260912-001'/f'{stem}.parquet'
    receipt=json.loads(parquet.with_suffix('.receipt.json').read_text(encoding='utf-8'))
    version=race['modelVersion']
    path=model_root/version/'model.json'
    metadata_path=path.with_name('metadata.json')
    metadata=json.loads(metadata_path.read_text(encoding='utf-8'))
    features=metadata['feature_names']
    missing=sorted(set(features)-set(pq.read_schema(parquet).names))
    request=_build_signed_request(r2=config,object_key=receipt['object_key'],method='HEAD')
    with urllib.request.urlopen(request,timeout=15) as response:
        safe_headers={name:response.headers.get(name) for name in ('ETag','Last-Modified','Content-Length','Date','x-amz-version-id')}
    (out/f'{stem}.headers.json').write_text(json.dumps(safe_headers,indent=2),encoding='utf-8')
    same_identity=(safe_headers['ETag'] or '').strip('"')==receipt['head_before']['identity']['etag']
    modified=parsedate_to_datetime(safe_headers['Last-Modified']) if safe_headers['Last-Modified'] else None
    generated=datetime.fromisoformat(race['predictionGeneratedAt'].replace('Z','+00:00'))
    entries=_load_cached_races(parquet)[race['raceId']]
    served_order=[int(p['horseNumber']) for p in sorted(race['prediction'],key=lambda p:p['rank'])]
    roster_equal=sorted(served_order)==sorted(int(e['umaban']) for e in entries)
    row={'race_id':race['raceId'],'model_version':version,'feature_count':len(features),'missing_features':missing,'roster_equal':roster_equal,'object_identity_stable':same_identity,'object_last_modified':None if modified is None else modified.isoformat(),'prediction_generated_at':generated.isoformat(),'object_not_newer_than_prediction':None if modified is None else modified<=generated,'metadata_sha256':hashlib.sha256(metadata_path.read_bytes()).hexdigest(),'parquet_sha256':hashlib.sha256(parquet.read_bytes()).hexdigest(),'production_eligible':False}
    if not missing and roster_equal and same_identity:
        if version not in models:
            model=CatBoost()
            model.load_model(str(path),format='json')
            models[version]=(model,hashlib.sha256(path.read_bytes()).hexdigest())
        model,model_hash=models[version]
        matrix=build_feature_matrix(entries,features,'catboost')
        # Same native raw formula and production matrix/rank helpers; bound threads explicitly.
        scores=np.asarray(model.predict(matrix,prediction_type='RawFormulaVal',thread_count=4),dtype=np.float64)
        if scores.ndim!=1 or scores.size!=len(entries) or not np.isfinite(scores).all():
            raise ValueError('Invalid native incumbent scores')
        ranked=rank_race_entries(entries,scores.tolist())
        replay_order=[horse.umaban for horse in ranked]
        row.update({'model_sha256':model_hash,'served_order':served_order,'native_replay_order':replay_order,'all_ranks_match':served_order==replay_order,'top5_match':served_order[:5]==replay_order[:5],'note':'selected-model direct replay; mismatch is not proof of a production bug, routing/postprocessing/model identity may require further tracing'})
    rows.append(row)
    (out/'report.json').write_text(json.dumps({'races':rows,'scope':'9/12 only; does not solve missing historical rich features or full PIT/served-byte identity','production_eligible':False},indent=2),encoding='utf-8')
    print('INCUMBENT_DAY_REPLAY',race['raceId'],'missing',len(missing),'roster',roster_equal,'object_before_prediction',row['object_not_newer_than_prediction'],'rank_match',row.get('all_ranks_match'),flush=True)
print('INCUMBENT_DAY_REPLAY_COMPLETE',len(rows),flush=True)
PY
