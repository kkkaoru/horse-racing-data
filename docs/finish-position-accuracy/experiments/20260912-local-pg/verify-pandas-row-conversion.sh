#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(git -C "$ROOT" rev-parse --show-toplevel)"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="$REPO/apps/pc-keiba-viewer/src/scripts:$REPO/apps/finish-position-predict-container/src"
uv run --no-sync --project "$REPO/apps/pc-keiba-viewer" python - "$ROOT" "$REPO" <<'PY'
import json
import sys
from pathlib import Path

import polars as pl
from catboost import CatBoost
from learning.frozen_production_evaluation import score_frozen_catboost

root, repo = map(Path, sys.argv[1:])
model_dir = repo / 'apps/finish-position-predict-container/models/finish-position/ban-ei/banei-cb-v9-sim-2011'
metadata = json.loads((model_dir / 'metadata.json').read_text(encoding='utf-8'))
model = CatBoost()
model.load_model(str(model_dir / 'model.json'), format='json')
checks = []
for mode in ('early', 'retrospective'):
    source = root / ('early-baseline-replay.parquet' if mode == 'early' else 'production-baseline-features/83/final/race_year=*/*.parquet')
    current = score_frozen_catboost(pl.read_parquet(source), model,
        training_end=metadata['train_date_range'][1], feature_names=tuple(metadata['feature_names']), frame_loader='pandas')
    saved = pl.read_parquet(root / f'frozen-pandas-{mode}-v1/83/predictions.parquet')
    equal = current.equals(saved)
    checks.append({'mode':mode,'rows':current.height,'entire_prediction_frame_equal':equal})
    if not equal:
        raise ValueError(f'Row-conversion change altered {mode} predictions')
report = {'purpose':'Verify dataframe row-conversion refactor against completed local-only replays',
          'new_training':False,'checks':checks}
(root / 'audit-pandas-row-conversion.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print(json.dumps(report,indent=2))
PY
