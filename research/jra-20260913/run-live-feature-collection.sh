#!/usr/bin/env bash
# Small, read-only R2 input capture; no model scoring or prediction writes.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
export PYTHONPATH="$ROOT/apps/finish-position-predict-container/src"
bash research/chronos2/run-local.sh "$ROOT/apps/timesfm-finish-position/.venv/bin/python" - <<'PY'
import hashlib
import json
import os
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from predict_lib.r2_client import r2_get_bytes, r2_head_object
from predict_lib.serve import R2Config

root=Path('research/jra-20260913')
out=root/'live-features-002'
out.mkdir(exist_ok=False)
config=R2Config(account_id=os.environ['R2_ACCOUNT_ID'],access_key_id=os.environ['R2_ACCESS_KEY_ID'],secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'],bucket='pc-keiba-features-archive')
features=json.loads((root/'marketfree-pilot-001/protocol.json').read_text(encoding='utf-8'))['feature_names']
receipts=[]
for venue in ('06','09'):
    for number in range(1,13):
        race=f'{number:02d}'
        key=f'feat-cache/catalog-v1/jra/20260913/{venue}/{race}/features.parquet'
        before=r2_head_object(config,key)
        data=r2_get_bytes(config,key,8*1024*1024)
        after=r2_head_object(config,key)
        receipt={'object_key':key,'received_at':datetime.now().astimezone().isoformat(),'found':data is not None,'read_only':True,'head_before':None if before is None else asdict(before),'head_after':None if after is None else asdict(after),'identity_stable':before is not None and after is not None and before.identity is not None and before.identity==after.identity}
        if data is not None:
            path=out/f'20260913-{venue}-{race}.parquet'
            path.write_bytes(data)
            table=pq.read_table(pa.BufferReader(data))
            receipt.update({'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data),'rows':table.num_rows,'columns':len(table.column_names),'missing_research_features':sorted(set(features)-set(table.column_names)),'roster':table.select(['race_id','umaban','ketto_toroku_bango']).to_pylist()})
        (out/f'20260913-{venue}-{race}.receipt.json').write_text(json.dumps(receipt,indent=2,default=str),encoding='utf-8')
        receipts.append(receipt)
        print('FEATURE_CAPTURE',venue,race,'found',data is not None,'stable',receipt['identity_stable'],flush=True)
(out/'summary.json').write_text(json.dumps({'races':receipts,'production_eligible':False},indent=2,default=str),encoding='utf-8')
print('LIVE_FEATURE_CAPTURE_COMPLETE',len(receipts),flush=True)
PY
