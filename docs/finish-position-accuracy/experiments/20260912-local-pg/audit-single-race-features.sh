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
import polars as pl
from predict_lib.scorer import build_feature_matrix

root = Path(sys.argv[1])
repo = Path(sys.argv[2])
production = Path.home() / '.local/share/horse-racing-data-production'
model = Path('apps/finish-position-predict-container/models/finish-position/ban-ei/banei-cb-v9-sim-2011')
metadata = json.loads((repo / model / 'metadata.json').read_text(encoding='utf-8'))
names = metadata['feature_names']
if not isinstance(names, list) or not all(isinstance(name, str) for name in names):
    raise ValueError('Feature names must be a list of strings')
paths = [root / run / '83/final/race_year=2026/data_0.parquet' for run in
         ('production-upcoming-features', 'production-upcoming-single-race')]
frames = [pl.read_parquet(path).filter(pl.col('race_bango').cast(pl.Int64) == 1)
          .with_columns(pl.col('umaban').cast(pl.Int64)).sort('umaban') for path in paths]
left, right = frames
keys = ['ketto_toroku_bango', 'umaban']
if left.is_empty() or not left.select(keys).equals(right.select(keys)):
    raise ValueError('Focused and card entrant keys must match exactly')
if any(name not in frame.columns for frame in frames for name in names):
    raise ValueError('Missing native model features')
a, b = [np.asarray(build_feature_matrix(frame.to_dicts(), names, 'catboost'), dtype=np.float64)
        for frame in frames]
equal = (a == b) | (np.isnan(a) & np.isnan(b))
files = [*[path.relative_to(repo) for path in (repo / model).iterdir() if path.is_file()],
         Path('apps/finish-position-predict-container/src/predict_lib/prophet_cell_policy.json'),
         Path('apps/finish-position-predict-container/src/predict_lib/prophet_adjustment.py'),
         Path('apps/finish-position-predict-container/src/predict_lib/pipeline_args.py'),
         Path('apps/finish-position-predict-container/src/predict_lib/late_binding.py'),
         Path('apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py'),
         Path('apps/pc-keiba-viewer/src/scripts/finish-position-features/add-similar-race-features.py'),
         Path('apps/pc-keiba-viewer/finish-position/lookups/prophet-entity-trends-all-categories-2026.parquet')]


def digest(path: Path) -> str | None:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None


report = {
    'purpose': 'Read-only local model-input and production-worktree comparison; not production parity or accuracy',
    'rows': left.height,
    'features': len(names),
    'matching_model_input_cells': int(equal.sum()),
    'total_model_input_cells': int(equal.size),
    'all_model_inputs_equal': bool(equal.all()),
    'projection': 'production build_feature_matrix, CatBoost float64, null-to-zero, NaN preserved',
    'different_features': [{'name': name, 'different_rows': int((~equal[:, index]).sum()),
                            'values': [{'horse_number': int(left['umaban'][int(row)]),
                                        'card': repr(float(a[row, index])),
                                        'focused': repr(float(b[row, index])),
                                        'absolute_difference': repr(float(abs(a[row, index] - b[row, index])))}
                                       for row in np.flatnonzero(~equal[:, index])]}
                           for index, name in enumerate(names) if not bool(equal[:, index].all())],
    'inputs': [{'path': str(path.relative_to(repo)), 'sha256': digest(path)} for path in paths],
    'production_worktree': str(production),
    'source_comparisons': [{'path': str(path), 'current_sha256': digest(repo / path),
                            'production_worktree_sha256': digest(production / path)} for path in files],
    'caveat': 'A local worktree is not attestation of the deployed image or its runtime environment. Final Prophet adjustment and actual serving input snapshots remain separate checks.',
}
(root / 'audit-single-race-features.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
print(json.dumps(report, indent=2))
PY
