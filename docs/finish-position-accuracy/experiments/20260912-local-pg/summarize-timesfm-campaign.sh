#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(git -C "$ROOT" rev-parse --show-toplevel)"
export PYTHONDONTWRITEBYTECODE=1
uv run --no-sync --project "$REPO/apps/timesfm-finish-position" python - "$ROOT" <<'PY'
import json
import sys
from itertools import product
from pathlib import Path

import numpy as np
import polars as pl
from timesfm_finish_position.nar_banei_exact import exact_hits

root = Path(sys.argv[1])
data = pl.read_parquet(root / 'timesfm-input-v2/targets.parquet')
cells = json.loads((root / 'target-cells.json').read_text(encoding='utf-8'))
selection = json.loads((root / 'final-family-selection-v1.json').read_text(encoding='utf-8'))
families = {item['cell']: item['family'] for item in selection['selected']}
reports = []
for cell, year in product(cells, range(2020, 2027)):
    cell_id = '-'.join(str(cell[name]) for name in ('category','venue','class_label','distance_band','season','surface'))
    frame = data.filter((pl.col('cell_id') == cell_id) & (pl.col('year') == year))
    races, dates = frame['race_id'].n_unique(), frame['race_date'].n_unique()
    baseline = exact_hits(frame, np.asarray(frame['baseline_score'].to_numpy(), dtype=np.float64))
    candidate = baseline.copy()
    prediction_path = None
    if cell_id in families and races:
        study = 'timesfm-final-selected-v1' if year == 2026 else f'timesfm-rustuna-{families[cell_id]}'
        prediction_path = root / study / 'timesfm' / cell_id / str(year) / 'predictions.parquet'
        predicted = pl.read_parquet(prediction_path)
        columns = ['race_id','horse_id','horse_number','race_date','finish']
        if not predicted.select(columns).sort('race_id','horse_id').equals(frame.select(columns).sort('race_id','horse_id')):
            raise ValueError('Selected prediction cohort must exactly match its control')
        saved_control = pl.read_parquet(prediction_path.with_name('baseline.parquet'))
        expected_control = frame.select(*columns, pl.col('baseline_score').alias('score'))
        if not saved_control.select(*columns, 'score').sort('race_id','horse_id').equals(expected_control.sort('race_id','horse_id')):
            raise ValueError('Saved baseline scores must match the immutable targets')
        candidate = exact_hits(predicted, np.asarray(predicted['score'].to_numpy(), dtype=np.float64))
    reports.append({
        'cell': cell_id, 'year': year, 'races': races, 'dates': dates, 'classified_entrants': frame.height,
        'observed_finish_rank_races': [frame.filter(pl.col('finish') == rank)['race_id'].n_unique() for rank in range(1,6)],
        'family': families.get(cell_id, 'unchanged-research-control'),
        'baseline_exact_hits': baseline.tolist(), 'candidate_exact_hits': candidate.tolist(),
        'baseline_exact_percent': (100 * baseline / races).tolist() if races else None,
        'candidate_exact_percent': (100 * candidate / races).tolist() if races else None,
        'delta_pp': (100 * (candidate - baseline) / races).tolist() if races else None,
        'predictions': str(prediction_path.relative_to(root)) if prediction_path is not None else None,
        'promotion_eligible': False,
    })
report = {
    'purpose': 'All ten target cells and all seven chronological years, including zero-support cohorts',
    'metric_denominator': 'All evaluation races, separately reporting races with each observed exact finish rank',
    'development_caveat': 'Family nomination used the entire 2021-2025 development series; this display is not nested family selection or new out-of-sample evidence',
    'final_caveat': '2026 families were frozen before TimesFM readout inspection, but broader 2026 campaign outcomes had already been used. Not a pristine holdout.',
    'serving_parity_verified': False, 'production_changed': False, 'rows': reports,
}
(root / 'timesfm-all-cell-year-summary-v1.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
print(json.dumps([row for row in reports if row['year'] == 2026], indent=2))
PY
