#!/usr/bin/env bash
# Read-only source/roster contract diagnostics; never rename inputs speculatively.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

root=Path('research/jra-20260913')
out=root/'roster-field-size-001'
out.mkdir(exist_ok=False)
predictions=json.loads((root/'cloudflare-003/predictions-20260913.json').read_text(encoding='utf-8'))
by_id={race['raceId']:race for race in predictions['races']}
rows=[]
for path in sorted((root/'live-features-002').glob('*.parquet')):
    frame=pd.read_parquet(path)
    ids=frame['race_id'].unique().tolist()
    if len(ids)!=1 or ids[0] not in by_id:
        raise ValueError('Missing original prediction race identity')
    race=by_id[ids[0]]
    original={int(h['horseNumber']) for h in race['prediction']}
    live={int(n) for n in frame['umaban']}
    alias=np.asarray(frame['shusso_tosu_1'],dtype=np.float64)
    normalized=np.asarray(frame['field_size_normalized'],dtype=np.float64)
    umaban=np.asarray(frame['umaban'],dtype=np.float64)
    expected_umaban=(umaban-1)/(alias-1)
    rows.append({'race_id':ids[0],'rows':len(frame),'original_prediction_rows':len(race['prediction']),'unique_original':len(original),'unique_live':len(live),'comparison_basis':'horse numbers only; saved prediction has no horse registration IDs and live parquet has no names','rosters_equal':original==live,'only_original':sorted(original-live),'only_live':sorted(live-original),'legacy_size_nonmissing':int(frame['shusso_tosu'].notna().sum()),'alias_values':sorted(set(alias.tolist())),'alias_equals_stored_rows':bool((alias==len(frame)).all()),'normalized_matches_alias':bool(np.isclose(normalized,np.minimum(alias/18,1),equal_nan=True).all()),'umaban_matches_alias':bool(np.isclose(np.asarray(frame['umaban_norm'],dtype=np.float64),expected_umaban,equal_nan=True).all())})
(out/'live.json').write_text(json.dumps(rows,indent=2),encoding='utf-8')
print('LIVE_ROSTER',sum(r['rosters_equal'] for r in rows),'/',len(rows),flush=True)
source=Path.home()/'.local/share/horse-racing-data-production/jra-cell-v1/features-2000-2026-mssd-sectional-relationship-all-history-dedup-v2'
dataset=ds.dataset(source,format='parquet',partitioning='hive')
columns=['shusso_tosu','field_size_normalized','umaban_norm','umaban']
frame=dataset.to_table(columns=columns).to_pandas()
size=np.asarray(frame['shusso_tosu'],dtype=np.float64)
normalized=np.asarray(frame['field_size_normalized'],dtype=np.float64)
known=np.isfinite(size)&(size>1)
expected=(np.asarray(frame['umaban'],dtype=np.float64)-1)/(size-1)
report={'rows':len(frame),'field_size_nonmissing':int(np.isfinite(size).sum()),'positive_field_size':int((size>0).sum()),'known_size_above_one':int(known.sum()),'normalized_agrees_with_size':int(np.isclose(normalized,np.minimum(size/18,1))[known].sum()),'umaban_agrees_with_size':int(np.isclose(np.asarray(frame['umaban_norm'],dtype=np.float64),expected)[known].sum()),'has_suffixed_size_column':'shusso_tosu_1' in dataset.schema.names,'no_input_mapping_changed':True,'production_eligible':False}
(out/'historical.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print('HISTORICAL_SIZE',report,flush=True)
PY
