#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(git -C "$ROOT" rev-parse --show-toplevel)"
export PYTHONPATH="$REPO/apps/pc-keiba-viewer/src/scripts"
uv run --project "$REPO/apps/pc-keiba-viewer" python - "$ROOT" "${YEARS:-2021,2022,2023,2024,2025}" "${EVALUATION_NAME:-development-v1}" "${STUDY_NAME:-timesfm-rustuna-v1}" "${EVALUATION_NOTE:-Development evidence; intervals do not correct the complete profile/cell/search family.}" <<'PY'
import json
import sys
from dataclasses import asdict
from pathlib import Path
import polars as pl
from learning.paired_rank_evaluation import compare_rank_predictions

root = Path(sys.argv[1])
years = [int(value) for value in sys.argv[2].split(',')]
output = root / f'timesfm-paired-{sys.argv[3]}'
output.mkdir(parents=True, exist_ok=True)
study = root / sys.argv[4]
summaries = []
for cell_path in sorted((study / 'timesfm').iterdir()):
    cell = cell_path.name
    selected_years = []
    for year in years:
        report = json.loads((cell_path / str(year) / 'report.json').read_text())
        if report['races']:
            selected_years.append(year)
    if not selected_years:
        summaries.append({'cell': cell, 'years': years, 'races': 0, 'promotion_eligible': False})
        continue
    candidate = pl.concat([pl.read_parquet(cell_path / str(year) / 'predictions.parquet') for year in selected_years])
    cell_output = output / cell
    cell_output.mkdir(parents=True, exist_ok=True)
    candidate.write_parquet(cell_output / 'timesfm.parquet')
    for origin in ('baseline', 'last', 'mean5'):
        base_paths = [(cell_path / str(year) / 'baseline.parquet') if origin == 'baseline' else (study / origin / cell / str(year) / 'predictions.parquet') for year in selected_years]
        baseline = pl.concat([pl.read_parquet(path) for path in base_paths])
        report = asdict(compare_rank_predictions(baseline, candidate))
        report.update({'cell': cell, 'years': years, 'control': origin, 'selection_note': sys.argv[5], 'production_comparison': False})
        (cell_output / f'versus-{origin}.json').write_text(json.dumps(report, indent=2))
        summaries.append({'cell': cell, 'control': origin, 'years': years, 'races': report['races'], 'dates': report['dates'], 'delta_pp': [round(value * 100, 3) for value in report['deltas']], 'lower_pp': None if report['lower'] is None else [round(value * 100, 3) for value in report['lower']], 'upper_pp': None if report['upper'] is None else [round(value * 100, 3) for value in report['upper']], 'promotion_eligible': False})
(output / 'summary.json').write_text(json.dumps(summaries, indent=2))
print(json.dumps(summaries, indent=2))
PY
