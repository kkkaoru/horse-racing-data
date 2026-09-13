#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
export PYTHONPATH="$ROOT/apps/finish-position-predict-container/src"
bash research/chronos2/run-local.sh "$ROOT/apps/timesfm-finish-position/.venv/bin/python" - <<'PY'
import json
import os
import hashlib
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from predict_lib.r2_client import r2_get_bytes
from predict_lib.serve import R2Config

root=Path('research/jra-20260913')
out=root/'feature-compatibility-001'
out.mkdir(exist_ok=False)
features=json.loads((root/'marketfree-pilot-001/protocol.json').read_text(encoding='utf-8'))['feature_names']
pa.set_cpu_count(4)
pa.set_io_thread_count(4)
live=[]
for path in sorted((root/'live-features-002').glob('*.parquet')):
    table=pq.read_table(path,columns=features)
    matrix=np.asarray(table.to_pandas(),dtype=np.float64)
    live.append({'file':path.name,'rows':len(matrix),'nan_cells':int(np.isnan(matrix).sum()),'infinite_cells':int(np.isinf(matrix).sum()),'null_columns':[name for name in features if table[name].null_count]})
(out/'live-nonfinite.json').write_text(json.dumps(live,indent=2),encoding='utf-8')
print('LIVE_NONFINITE_CELLS',sum(row['nan_cells'] for row in live),sum(row['infinite_cells'] for row in live),flush=True)
config=R2Config(account_id=os.environ['R2_ACCOUNT_ID'],access_key_id=os.environ['R2_ACCESS_KEY_ID'],secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'],bucket='pc-keiba-features-archive')
key='feat-cache/catalog-v1/jra/20260905/06/01/features.parquet'
data=r2_get_bytes(config,key,8*1024*1024)
if data is None:
    (out/'historical-object-absent.json').write_text(json.dumps({'key':key,'found':False}),encoding='utf-8')
    print('HISTORICAL_OBJECT_ABSENT',flush=True)
else:
    (out/'20260905-06-01.parquet').write_bytes(data)
    remote=pq.read_table(pa.BufferReader(data)).to_pandas()
    available=[name for name in features if name in remote.columns]
    source=Path('/Users/kkk4oru/.local/share/horse-racing-data-production/jra-cell-v1/features-2000-2026-mssd-sectional-relationship-all-history-dedup-v2')
    local=ds.dataset(source,format='parquet',partitioning='hive').to_table(columns=['race_id','ketto_toroku_bango','umaban',*available],filter=ds.field('race_id')=='jra:2026:0905:06:01').to_pandas()
    joined=local.merge(remote,on=['race_id','ketto_toroku_bango','umaban'],suffixes=('_local','_remote'),validate='one_to_one')
    details=[]
    for name in available:
        a=np.asarray(joined[name+'_local'],dtype=np.float64)
        b=np.asarray(joined[name+'_remote'],dtype=np.float64)
        valid=np.isfinite(a)&np.isfinite(b)
        equal=np.isclose(a,b,rtol=1e-6,atol=1e-6,equal_nan=True)
        details.append({'feature':name,'equal_rows':int(equal.sum()),'rows':len(a),'max_absolute_difference':float(np.max(np.abs(a[valid]-b[valid]))) if valid.any() else None,'local':a.tolist(),'remote':b.tolist()})
    report={'source_key':key,'remote_sha256':hashlib.sha256(data).hexdigest(),'local_rows':len(local),'remote_rows':len(remote),'matched_rows':len(joined),'missing_remote_features':sorted(set(features)-set(available)),'different_features':[item['feature'] for item in details if item['equal_rows']!=item['rows']],'details':details,'production_eligible':False}
    (out/'comparison.json').write_text(json.dumps(report,indent=2,allow_nan=True),encoding='utf-8')
    print('FEATURE_COMPARISON',len(joined),'rows',len(report['different_features']),'different features',flush=True)
PY
