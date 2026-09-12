#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(git -C "$ROOT" rev-parse --show-toplevel)"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="$REPO/apps/finish-position-predict-container/src"
uv run --no-sync --project "$REPO/apps/pc-keiba-viewer" python - "$ROOT" "$REPO" "${LOCAL_RUN:-production-upcoming-features}" "${REPORT_NAME:-audit-captured-foundation.json}" "${REMOTE_PARQUET:-}" "${FRAME_LOADER:-polars}" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
from catboost import CatBoost
from predict_lib.prophet_adjustment import adjust_prediction_rows_with_prophet
from predict_lib.scorer import build_feature_matrix
from predict_lib.upcoming import build_prediction_rows, rank_race_entries

root, repo = Path(sys.argv[1]), Path(sys.argv[2])
model_dir = repo / 'apps/finish-position-predict-container/models/finish-position/ban-ei/banei-cb-v9-sim-2011'
names = json.loads((model_dir / 'metadata.json').read_text(encoding='utf-8'))['feature_names']
capture = json.loads((root / 'cloudflare-r1-foundation.json').read_text(encoding='utf-8'))
manifest = json.loads((root / 'cloudflare-day-foundation-manifest.json').read_text(encoding='utf-8'))
if capture['source'] != manifest['source'] or capture['contract']['generationId'] != manifest['contract']['generationId']:
    raise ValueError('Foundation and manifest do not describe the same source generation')
if sys.argv[6] not in ('polars', 'pandas'):
    raise ValueError('Unsupported frame loader')
local_path = root / sys.argv[3] / '83/final/race_year=2026/data_0.parquet'
if sys.argv[6] == 'pandas':
    local_frame = pd.read_parquet(local_path)
    local = local_frame.loc[local_frame['race_bango'].astype(int) == 1].to_dict(orient='records')
else:
    local = pl.read_parquet(local_path).filter(pl.col('race_bango').cast(pl.Int64) == 1).to_dicts()
remote_rows = capture['rows']
if sys.argv[5]:
    remote_path = root / sys.argv[5]
    remote_rows = (pd.read_parquet(remote_path).to_dict(orient='records') if sys.argv[6] == 'pandas'
                   else pl.read_parquet(remote_path).to_dicts())
remote = sorted(remote_rows, key=lambda row: int(row['umaban']))
local = sorted(local, key=lambda row: int(row['umaban']))
remote_keys = [(str(row['ketto_toroku_bango']), int(row['umaban'])) for row in remote]
local_keys = [(str(row['ketto_toroku_bango']), int(row['umaban'])) for row in local]
if remote_keys != local_keys or len(remote_keys) != len(set(remote_keys)):
    raise ValueError('Captured and local entrant keys must match uniquely')
a, b = [np.asarray(build_feature_matrix(rows, names, 'catboost'), dtype=np.float64) for rows in (local, remote)]
equal = (a == b) | (np.isnan(a) & np.isnan(b))
model = CatBoost()
model.load_model(str(model_dir / 'model.json'), format='json')
local_scores, remote_scores = model.predict(a), model.predict(b)
version = 'banei-cb-v9-sim-2011'
rows = build_prediction_rows(capture['raceId'], 'ban-ei', rank_race_entries(remote, remote_scores), version, remote[0], entries=remote)
wrapper = json.loads((root / 'cloudflare-banei-rank-parity.response.json').read_text(encoding='utf-8'))
production = next(race for race in json.loads(wrapper['data']['content'][0]['text'])['values'] if int(race['race']) == 1)
production_order = [int(row['horseNumber']) for row in sorted(production['rows'], key=lambda row: float(row['norm']))]
replays = []
for cell in ('sim', 'base'):
    adjusted = adjust_prediction_rows_with_prophet(rows, remote, 'ban-ei', {}, cell_variant=cell, branch_variant=version,
        served_signature=f'v1;mode=direct-default;stage2=direct-default;stage2-model={version};stage1=not-configured;final={version}')
    order = [int(row[7]) for row in adjusted.rows]
    replays.append({'assumed_cell': cell, 'applied': adjusted.applied, 'reason': adjusted.reason,
                    'order': order, 'production_rank_matches': sum(a == b for a,b in zip(order, production_order, strict=True))})
report = {
    'purpose': 'Captured current-card foundation inference audit only; NEVER training data',
    'source': capture['source'], 'contract': capture['contract'], 'catalog_source_hash': capture['catalogSourceHash'],
    'source_identity_scope': 'Day-base reference only; final-cache linkage is not attested by these fields',
    'prediction_generated_at': production['generated'],
    'snapshot_vintage_attested_to_prediction': False,
    'model_sha256': hashlib.sha256((model_dir / 'model.json').read_bytes()).hexdigest(),
    'local_feature_run': sys.argv[3],
    'frame_loader': sys.argv[6],
    'remote_feature_input': sys.argv[5] or 'cloudflare-r1-foundation.json',
    'rows': len(remote), 'features': len(names), 'matching_input_cells': int(equal.sum()),
    'absent_foundation_features': [name for name in names if name not in remote[0]],
    'different_features': [
        {'name': name, 'rows': [{'horse_number': remote_keys[int(row)][1],
                               'local': repr(float(a[row,index])), 'captured': repr(float(b[row,index]))}
                              for row in np.flatnonzero(~equal[:,index])]}
        for index,name in enumerate(names) if not bool(equal[:,index].all())
    ],
    'local_order': [horse.umaban for horse in rank_race_entries(local, local_scores)],
    'captured_raw_order': [int(row[7]) for row in rows], 'production_order': production_order,
    'captured_policy_replays': replays,
    'scores': [{'horse_number': key[1], 'local': float(a), 'captured': float(b)}
               for key,a,b in zip(remote_keys, local_scores, remote_scores, strict=True)],
    'accuracy_metrics': None, 'promotion_eligible': False,
    'caveat': 'Day-base foundation precedes runtime late binding. Current model/policy and cached source generation are not yet attested to the published prediction.',
}
(root / sys.argv[4]).write_text(json.dumps(report, indent=2), encoding='utf-8')
print(json.dumps(report, indent=2))
PY
