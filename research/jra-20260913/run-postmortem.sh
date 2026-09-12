#!/usr/bin/env bash
# Reproduce exact-rank analysis from retained Cloudflare responses only.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
export PYTHONPATH="$ROOT/research/jra-20260913"
export COVERAGE_FILE="$ROOT/research/jra-20260913/logs/review-coverage-001"
PY="$ROOT/apps/timesfm-finish-position/.venv/bin"
FILES=(research/jra-20260913/race_rank_review.py research/jra-20260913/test_race_rank_review.py)
bash research/chronos2/run-local.sh "$PY/ruff" check --config apps/finish-position-predict-container/pyproject.toml "${FILES[@]}"
bash research/chronos2/run-local.sh "$PY/ruff" format --check --config apps/finish-position-predict-container/pyproject.toml "${FILES[@]}"
bash research/chronos2/run-local.sh "$PY/basedpyright" --project apps/finish-position-predict-container/pyproject.toml --pythonpath "$PY/python" "${FILES[@]}"
bash research/chronos2/run-local.sh "$PY/pytest" -p no:cacheprovider --basetemp=research/jra-20260913/test-review-001 --cov=race_rank_review --cov-config=apps/finish-position-predict-container/pyproject.toml --cov-report=term-missing --cov-report=json:research/jra-20260913/logs/review-coverage-001.json --cov-fail-under=95 research/jra-20260913/test_race_rank_review.py
bash research/chronos2/run-local.sh "$PY/python" - <<'PY'
import hashlib
import json
import math
import unicodedata
from dataclasses import asdict
from datetime import datetime
from itertools import chain
from operator import itemgetter
from pathlib import Path
from zoneinfo import ZoneInfo

from race_rank_review import MarketPoint, market_at_cutoff, review_orders

ROOT = Path('research/jra-20260913')
OUT = ROOT / 'postmortem-001'
OUT.mkdir(exist_ok=False)
PREDICTIONS_PATH = ROOT / 'cloudflare-002/predictions-20260912.json'
PLAN_PATH = ROOT / 'source-snapshot-001/plan-20260912.json'
PREDICTIONS = json.loads(PREDICTIONS_PATH.read_text(encoding='utf-8'))
PLAN = json.loads(PLAN_PATH.read_text(encoding='utf-8'))
CELL_BY_RACE = dict(chain.from_iterable(
    ((race_id, cell['cell_id']) for race_id in cell['target_race_ids'])
    for cell in PLAN['cells']
))
if {race['raceId'] for race in PREDICTIONS['races']} != set(CELL_BY_RACE):
    raise ValueError('Cloudflare prediction and target race inventories disagree')
REVIEWS = []
INPUTS = [PREDICTIONS_PATH, PLAN_PATH]
for race in PREDICTIONS['races']:
    race_id = race['raceId']
    parts = race_id.split(':')
    realtime_path = ROOT / 'cloudflare-realtime-001' / f'{parts[1]}{parts[2]}-{parts[3]}-{parts[4]}.json'
    INPUTS.append(realtime_path)
    realtime = json.loads(realtime_path.read_text(encoding='utf-8'))
    if realtime['raceKey'] != race_id or not realtime['raceResults']:
        raise ValueError(f'Missing or mismatched Cloudflare results: {race_id}')
    results = realtime['raceResults']['horses']
    predicted = sorted(race['prediction'], key=itemgetter('rank'))
    order = tuple(int(horse['horseNumber']) for horse in predicted)
    actual = {
        int(horse['horseNumber']): (
            int(horse['finishPosition'])
            if str(horse['finishPosition']).isdecimal() and int(horse['finishPosition']) > 0
            else None
        ) for horse in results
    }
    if len(actual) != len(results):
        raise ValueError(f'Duplicate Cloudflare result horse: {race_id}')
    forecast_time = datetime.fromisoformat(race['predictionGeneratedAt'])
    start = datetime.strptime(parts[1] + parts[2] + race['startTime'], '%Y%m%d%H%M').replace(tzinfo=ZoneInfo('Asia/Tokyo'))
    reasons = []
    if forecast_time >= start:
        reasons.append('prediction_not_before_scheduled_start')
    if [horse['rank'] for horse in predicted] != list(range(1, len(order) + 1)):
        reasons.append('prediction_ranks_not_consecutive')
    if {horse['predictionGeneratedAt'] for horse in predicted} != {race['predictionGeneratedAt']}:
        reasons.append('mixed_prediction_timestamps')
    names_model = {int(h['horseNumber']): ''.join(unicodedata.normalize('NFKC', h['horseName']).split()) for h in predicted}
    names_result = {int(h['horseNumber']): ''.join(unicodedata.normalize('NFKC', h['horseName']).split()) for h in results}
    if names_model != names_result:
        reasons.append('prediction_result_roster_or_name_mismatch')
    quotes = list(chain.from_iterable(horse['points'] for horse in realtime['odds']['history']))
    valid_quotes = [point for point in quotes if isinstance(point.get('odds'), (int, float)) and math.isfinite(point['odds']) and point['odds'] > 0]
    points = [MarketPoint(int(p['horseNumber']), datetime.fromisoformat(p['fetchedAt']), float(p['odds'])) for p in valid_quotes]
    asof_market = market_at_cutoff(points=points, horses=order, cutoff=forecast_time)
    final_quotes = realtime['odds']['latest']['tansho']
    final_points = [MarketPoint(int(p['combination']), datetime.fromisoformat(realtime['odds']['fetchedAt']), float(p['odds'])) for p in final_quotes if isinstance(p.get('odds'), (int, float)) and math.isfinite(p['odds']) and p['odds'] > 0]
    final_market = market_at_cutoff(points=final_points, horses=order, cutoff=datetime.fromisoformat(realtime['odds']['fetchedAt']))
    metrics = None
    final_metrics = None
    try:
        metrics = asdict(review_orders(predicted=order, actual=actual, market=asof_market))
        final_metrics = asdict(review_orders(predicted=order, actual=actual, market=final_market))
    except ValueError as error:
        reasons.append(str(error))
    record = {'race_id': race_id, 'cell_id': CELL_BY_RACE[race_id], 'model_version': race['modelVersion'], 'forecast_generated_at': race['predictionGeneratedAt'], 'scheduled_start': start.isoformat(), 'results_fetched_at': realtime['raceResults']['fetchedAt'], 'prediction': order, 'actual_finishes': actual, 'market_at_prediction': asof_market, 'market_latest': final_market, 'metrics_at_prediction': metrics, 'metrics_latest_market': final_metrics, 'primary_eligible': not reasons, 'reasons': reasons, 'invalid_market_quotes': len(quotes) - len(valid_quotes), 'track_condition': realtime['trackCondition']}
    REVIEWS.append(record)
    print('RACE_REVIEW', race_id, 'eligible', not reasons, 'hits', metrics['model_hits'] if metrics else None, 'market', metrics['market_hits'] if metrics else None, 'issues', reasons, flush=True)
PRIMARY = [r for r in REVIEWS if r['primary_eligible'] and r['metrics_at_prediction'] is not None]
PAIRED = [r for r in PRIMARY if r['metrics_at_prediction']['market_hits'] is not None]
SUMMARY = {'race_count': len(REVIEWS), 'primary_races': len(PRIMARY), 'paired_asof_market_races': len(PAIRED), 'model_hits': [sum(r['metrics_at_prediction']['model_hits'][rank] for r in PRIMARY) for rank in range(5)], 'rank_support': [sum(r['metrics_at_prediction']['support'][rank] for r in PRIMARY) for rank in range(5)], 'paired_model_minus_asof_market': [sum(r['metrics_at_prediction']['model_minus_market'][rank] for r in PAIRED) for rank in range(5)], 'identical_asof_market_orders': sum(r['metrics_at_prediction']['identical_market_order'] for r in PAIRED), 'reviews': REVIEWS, 'source_of_actual_results': 'Cloudflare sync-realtime-data raceResults', 'production_eligible': False, 'input_hashes': [{'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()} for path in INPUTS]}
(OUT / 'summary.json').write_text(json.dumps(SUMMARY, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
print('POSTMORTEM_COMPLETE', {k:v for k,v in SUMMARY.items() if k not in ('reviews', 'input_hashes')}, flush=True)
PY
