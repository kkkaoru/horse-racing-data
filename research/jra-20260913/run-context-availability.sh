#!/usr/bin/env bash
# Check reusable production pace/context formulas against the captured live inputs.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import hashlib
import json
import re
import runpy
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

root=Path('research/jra-20260913')
out=root/'context-availability-001'
out.mkdir(exist_ok=False)
source=Path('apps/pc-keiba-viewer/src/scripts/finish-position-features/add-race-internal-features.py')
namespace=runpy.run_path(str(source))
builder=namespace['append_features_sql']
if not callable(builder):
    raise TypeError('Canonical SQL builder unavailable')
sql=builder('__registered_input__')
if not isinstance(sql,str):
    raise TypeError('Canonical builder must return SQL text')
reader="read_parquet('__registered_input__', hive_partitioning=true)"
if sql.count(reader)!=1:
    raise ValueError('Unexpected canonical input relation')
sql=sql.replace(reader,'canonical_input')
(out/'canonical-query.sql').write_text(sql,encoding='utf-8')
fields=('field_nige_pressure','field_senkou_pressure','field_sashi_pressure','field_oikomi_pressure','field_pace_index','field_nige_candidate_count','self_nige_rate_minus_field_avg','umaban_x_nige_history','field_avg_speed_index','field_top_speed_index','field_avg_career_win_rate','field_avg_past_kohan_3f','field_avg_past_corner_1_norm','field_max_past_corner_1_norm','field_min_past_corner_1_norm','field_spread_past_corner_1_norm','field_has_pure_nige_horse','self_style_dominant_rate','field_avg_style_concentration','field_style_diversity')
keys=['race_id','ketto_toroku_bango','umaban']
required=sorted(set(re.findall(r'\bb\.([a-z][a-z0-9_]*)',sql)) | set(keys))
frame=pd.concat([pd.read_parquet(path) for path in sorted((root/'live-features-002').glob('*.parquet'))],ignore_index=True)
missing=sorted(set(required)-set(frame.columns))
if missing:
    raise ValueError(f'Canonical base inputs unavailable: {missing}')
with duckdb.connect() as connection:
    connection.execute('SET threads=4')
    connection.register('canonical_input',frame.loc[:,required])
    derived=connection.execute(sql).fetchdf()
paired=frame.loc[:,keys+list(fields)].merge(derived.loc[:,keys+list(fields)],on=keys,suffixes=('_stored','_derived'),validate='one_to_one')
if len(paired)!=len(frame):
    raise ValueError('Live roster changed during comparison')
results=[]
for field in fields:
    a=np.asarray(paired[field+'_stored'],dtype=np.float64)
    b=np.asarray(paired[field+'_derived'],dtype=np.float64)
    equal=np.isclose(a,b,atol=1e-6,rtol=1e-6,equal_nan=True)
    results.append({'feature':field,'equal_rows':int(equal.sum()),'rows':len(equal),'different_horses':paired.loc[~equal,keys].to_dict(orient='records')})
report={'source_path':str(source),'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'base_columns':required,'features':fields,'rows':len(frame),'comparison':results,'all_equal':all(r['equal_rows']==r['rows'] for r in results),'production_eligible':False}
(out/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print('CANONICAL_CONTEXT_PARITY',report['all_equal'],[(r['feature'],r['rows']-r['equal_rows']) for r in results if r['rows']!=r['equal_rows']],flush=True)
PY
