#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(git -C "$ROOT" rev-parse --show-toplevel)"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="$REPO/apps/finish-position-predict-container/src"
uv run --no-sync --project "$REPO/apps/pc-keiba-viewer" python - "$ROOT" "$REPO" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from catboost import CatBoost
from predict_lib.prophet_adjustment import adjust_prediction_rows_with_prophet
from predict_lib.scorer import build_feature_matrix
from predict_lib.upcoming import build_prediction_rows, rank_race_entries

root, repo = map(Path, sys.argv[1:])
wrapper = json.loads((root / 'cloudflare-full-card-provenance.response.json').read_text(encoding='utf-8'))
if not wrapper.get('ok'):
    raise ValueError('Provenance retrieval failed')
provenance = json.loads(wrapper['data']['content'][0]['text'])
version = 'banei-cb-v9-sim-2011'
model_dir = repo / 'apps/finish-position-predict-container/models/finish-position/ban-ei' / version
names = json.loads((model_dir / 'metadata.json').read_text(encoding='utf-8'))['feature_names']
model = CatBoost()
model.load_model(str(model_dir / 'model.json'), format='json')
reports = []
for published in provenance['races']:
    race = published['race']
    source = root / 'cloudflare-serving-card' / f'{race}.parquet'
    entries = pd.read_parquet(source).sort_values('umaban').to_dict(orient='records')
    race_id = f'nar:2026:0912:83:{race}'
    if {row['race_id'] for row in entries} != {race_id} or published['models'] != [version]:
        raise ValueError('Unexpected race or published model')
    keys = [int(row['umaban']) for row in entries]
    expected = [int(row['horseNumber']) for row in sorted(published['rows'], key=lambda row: float(row['predictedFinishNorm']))]
    if len(keys) != len(set(keys)) or sorted(keys) != sorted(expected):
        raise ValueError('Published and cached cohorts must match uniquely')
    matrix = build_feature_matrix(entries, names, 'catboost')
    scores = np.asarray(model.predict(matrix), dtype=np.float64)
    if not np.isfinite(scores).all():
        raise ValueError('Nonfinite model scores')
    rows = build_prediction_rows(race_id, 'ban-ei', rank_race_entries(entries, scores), version, entries[0], entries=entries)
    policies = []
    for cell in ('sim', 'base'):
        adjusted = adjust_prediction_rows_with_prophet(rows, entries, 'ban-ei', {}, cell_variant=cell, branch_variant=version,
            served_signature=f'v1;mode=direct-default;stage2=direct-default;stage2-model={version};stage1=not-configured;final={version}')
        order = [int(row[7]) for row in adjusted.rows]
        sample_stddev = float(np.std([float(row[8]) for row in adjusted.rows], ddof=1))
        policies.append({'assumed_cell': cell, 'applied': adjusted.applied, 'reason': adjusted.reason,
                         'adjusted_score_sample_stddev': sample_stddev,
                         'published_stddev_max_abs_delta': max(abs(sample_stddev - float(value)) for value in published['scoreStddevs']),
                         'order': order, 'rank_matches': sum(a == b for a,b in zip(order, expected, strict=True)),
                         'adjusted_score_stddev': float(np.std([float(row[8]) for row in adjusted.rows]))})
    reports.append({'race': race, 'rows': len(entries), 'cache_sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
                    'generated': published['generated'], 'published_score_stddevs': published['scoreStddevs'],
                    'raw_score_stddev': float(np.std(scores)), 'raw_order': [int(row[7]) for row in rows],
                    'published_order': expected, 'policies': policies})
if len(reports) != 12 or sum(report['rows'] for report in reports) != 120:
    raise ValueError('Expected the complete 12-race, 120-runner Ban-ei card')
report = {'purpose': 'Read-only captured serving-input replay; NEVER training data',
          'frame_loader': 'pandas.read_parquet -> to_dict(records)',
          'published_stddev_contract': 'prediction-kv-writer.ts uses sample standard deviation (ddof=1)',
          'model_sha256': hashlib.sha256((model_dir / 'model.json').read_bytes()).hexdigest(),
          'races': reports, 'objects': provenance['objects'],
          'accuracy_metrics': None, 'promotion_eligible': False,
          'deployed_image_bytes_attested': False,
          'caveat': 'Agreement is inference replay, not accuracy gain. Policy alternatives are diagnostics, not per-race selections. Cache timestamps do not cryptographically attest deployment or source vintage.'}
(root / 'audit-serving-card.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
print(json.dumps(report, indent=2))
PY
