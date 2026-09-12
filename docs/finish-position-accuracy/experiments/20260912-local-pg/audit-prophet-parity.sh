#!/usr/bin/env bash
# Offline policy sensitivity only: no prediction writes to any production service.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(git -C "$ROOT" rev-parse --show-toplevel)"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="$REPO/apps/finish-position-predict-container/src"
uv run --no-sync --project "$REPO/apps/pc-keiba-viewer" python - "$ROOT" <<'PY'
import json
import sys
from pathlib import Path

import polars as pl
from predict_lib.prophet_adjustment import adjust_prediction_rows_with_prophet, configured_prophet_weight
from predict_lib.upcoming import build_prediction_rows, rank_race_entries

root = Path(sys.argv[1])
features = pl.read_parquet(root / 'production-upcoming-prophet-v1/83/final/race_year=2026/data_0.parquet')
scores = pl.read_parquet(root / 'local-upcoming-frozen-v1/83/predictions.parquet')
wrapper = json.loads((root / 'cloudflare-banei-rank-parity.response.json').read_text(encoding='utf-8'))
snapshot = json.loads(wrapper['data']['content'][0]['text'])
version = 'banei-cb-v9-sim-2011'
signature = f'v1;mode=direct-default;stage2=direct-default;stage2-model={version};stage1=not-configured;final={version}'
reports = []
for race in snapshot['values']:
    number = int(race['race'])
    race_id = f'nar-20260912-83-{number:02d}'
    entries = features.filter(pl.col('race_bango').cast(pl.Int64) == number).to_dicts()
    race_scores = scores.filter(pl.col('race_id') == race_id)
    by_horse = dict(zip(race_scores['horse_number'].to_list(), race_scores['score'].to_list(), strict=True))
    production_order = [int(row['horseNumber']) for row in sorted(race['rows'], key=lambda row: float(row['norm']))]
    if not entries or len(entries) != len(by_horse) or sorted(by_horse) != sorted(production_order):
        raise ValueError('Production and local entrant cohorts must agree')
    ranked = rank_race_entries(entries, [by_horse[int(entry['umaban'])] for entry in entries])
    # Research IDs use hyphens; the serving row builder requires five colon-separated fields.
    serving_race_id = f'nar:2026:0912:83:{number:02d}'
    raw_rows = build_prediction_rows(serving_race_id, 'ban-ei', ranked, version, entries[0], entries=entries)
    raw_order = [int(row[7]) for row in raw_rows]
    for cell in ('sim', 'base'):
        adjusted = adjust_prediction_rows_with_prophet(
            raw_rows, entries, 'ban-ei', {}, cell_variant=cell,
            branch_variant=version, served_signature=signature,
        )
        order = [int(row[7]) for row in adjusted.rows]
        reports.append({
            'race': number, 'assumed_cell': cell, 'rows': len(order),
            'weight': configured_prophet_weight('ban-ei', {}, cell_variant=cell,
                                               branch_variant=version, served_signature=signature),
            'applied': adjusted.applied, 'reason': adjusted.reason,
            'raw_production_matches': sum(a == b for a, b in zip(raw_order, production_order, strict=True)),
            'adjusted_production_matches': sum(a == b for a, b in zip(order, production_order, strict=True)),
            'adjusted_raw_matches': sum(a == b for a, b in zip(order, raw_order, strict=True)),
            'raw_order': raw_order, 'adjusted_order': order, 'production_order': production_order,
        })
report = {
    'purpose': 'Offline inference-only sensitivity to both current Prophet cell policies; no external training data',
    'policy_environment': 'Explicit empty environment, not attested deployed overrides',
    'served_signature_assumption': signature,
    'actual_cell_routing_attested': False,
    'accuracy_metrics': None,
    'promotion_eligible': False,
    'results': reports,
}
(root / 'audit-prophet-parity.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
print(json.dumps(report, indent=2))
PY
