#!/usr/bin/env bash
# One fixed 2023 development fit per cell, with market-free features and full source rosters.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
set -a
# shellcheck source=/dev/null
source apps/local-postgresql/.env
set +a
export PGHOST=127.0.0.1 PGPORT="${POSTGRES_PORT:?}" PGUSER="${POSTGRES_USER:?}"
export PGPASSWORD="${POSTGRES_PASSWORD:?}" PGDATABASE="${POSTGRES_DB:?}" PGCONNECT_TIMEOUT=10
export PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=90000'
export PYTHONPATH="$ROOT/research/jra-20260913:$ROOT/apps/finish-position-predict-container/src"
export OMP_NUM_THREADS=4 OPENBLAS_NUM_THREADS=4 MKL_NUM_THREADS=4
bash research/chronos2/run-local.sh "$ROOT/apps/timesfm-finish-position/.venv/bin/python" - <<'PY'
import hashlib
import json
import time
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from catboost import CatBoostRanker

from build_jra_cell_manifest import load_races
from predict_lib.jra_cell_scope import JraRaceIndex
from dedicated_cells import CHALLENGE, ST_LITE, event_fold
from fold_ranker import RankerConfig, fit_and_predict, market_free_features, relevance_for_top5
from race_rank_review import review_orders

ROOT = Path('research/jra-20260913')
OUT = ROOT / 'marketfree-pilot-001'
OUT.mkdir(exist_ok=False)
DEADLINE = datetime(2026,9,13,9,tzinfo=ZoneInfo('Asia/Tokyo'))
CONFIG = RankerConfig()
SOURCE = Path('/Users/kkk4oru/.local/share/horse-racing-data-production/jra-cell-v1/features-2000-2026-mssd-sectional-relationship-all-history-dedup-v2')
STATUS = ROOT / 'training-status-002/jvd-runner-status.parquet'
SCHEMA_PATH = ROOT / 'feature-audit-001/schema.json'
FEATURES = market_free_features(json.loads(SCHEMA_PATH.read_text(encoding='utf-8'))['numeric_feature_names'])
PROTOCOL = {'configuration':asdict(CONFIG),'feature_names':FEATURES,'loss':'YetiRank','relevance':'max(6-rank,0); explicit DNF/DQ relevance zero without fabricated finishing rank','maximum_fits':24,'evaluation_year':2023,'training_rule':'cell/event 20-year entrant history across venues; complete active source race rosters, structural unavailable features remain NaN','market_used_in_training':False,'source_scope':'JVD; NVD cross-source completion remains pending','same_day_unproven_features_excluded':True,'missing_feature_policy':'NaN, never omit individual training runners; target feature coverage remains a deployment gate','tie_break':'descending score, horse registration, horse number','production_eligible':False,'deadline':DEADLINE.isoformat(),'input_status_sha256':json.loads((STATUS.parent/'receipt.json').read_text(encoding='utf-8'))['sha256'],'dedicated_cells':[asdict(ST_LITE),asdict(CHALLENGE)]}
(OUT/'protocol.json').write_text(json.dumps(PROTOCOL,ensure_ascii=False,indent=2),encoding='utf-8')
print('PROTOCOL_FROZEN',len(FEATURES),'features',flush=True)
RACES = load_races('', '20000101', '20260913')
INDEX = JraRaceIndex(RACES)
CASES = [asdict(event_fold(cell=cell,races=RACES,index=INDEX,year=2023,observed_before=date(2026,9,13))) for cell in (ST_LITE,CHALLENGE)]
for path in sorted((ROOT/'fold-scopes-001').glob('jra-cell-*.json')):
    payload = json.loads(path.read_text(encoding='utf-8'))
    if payload['cell_id'] in (ST_LITE.routing_cell_id,CHALLENGE.routing_cell_id):
        continue
    fold = next(f for f in payload['folds'] if f['evaluation_year']==2023)
    CASES.append({'cell_id':payload['cell_id'], **fold})
if len(CASES)!=24:
    raise ValueError('Unexpected cell inventory')
(OUT/'cohorts.json').write_text(json.dumps(CASES,indent=2,default=str),encoding='utf-8')
pa.set_cpu_count(4)
pa.set_io_thread_count(4)
status_frame = pd.read_parquet(STATUS)
status_frame['horse_number'] = pd.to_numeric(status_frame['horse_number'],errors='raise').astype(np.int32)
status_frame = status_frame.loc[~status_frame['abnormality_code'].isin(['1','2','3'])].copy()
identity = ['race_id','horse_id','horse_number']
feature_table = ds.dataset(SOURCE,format='parquet',partitioning='hive').to_table(columns=['race_id','ketto_toroku_bango','umaban','tansho_odds',*FEATURES])
feature_frame = feature_table.to_pandas().rename(columns={'ketto_toroku_bango':'horse_id','umaban':'horse_number'})
feature_frame['feature_row_present'] = True
if feature_frame.duplicated(identity).any():
    raise ValueError('Feature identities must be unique')
frame = status_frame.merge(feature_frame,on=identity,how='left',validate='one_to_one').sort_values(identity)
del feature_frame, feature_table, status_frame
frame['actual_finish'] = pd.to_numeric(frame['finish_text'],errors='coerce')
frame.loc[frame['abnormality_code'].isin(['4','5']),'actual_finish'] = np.nan
print('COMPLETE_SOURCE_ROSTER_LOADED',len(frame),'rows','missing feature rows',int(frame['feature_row_present'].isna().sum()),flush=True)
SUMMARIES = []
for case in CASES:
    if datetime.now(ZoneInfo('Asia/Tokyo')) >= DEADLINE:
        print('DEADLINE_REACHED_NO_NEW_FITS',flush=True)
        break
    cell_id = case['cell_id']
    if not case['evaluation_race_ids']:
        SUMMARIES.append({'cell_id':cell_id,'status':'no-exact-development-races','production_eligible':False})
        print('NO_DEVELOPMENT_RACES',cell_id,flush=True)
        continue
    started = time.monotonic()
    train = frame.loc[frame['race_id'].isin(case['training_race_ids'])]
    evaluation = frame.loc[frame['race_id'].isin(case['evaluation_race_ids'])].copy()
    if set(train['race_id']) != set(case['training_race_ids']) or set(evaluation['race_id']) != set(case['evaluation_race_ids']):
        raise ValueError(f'Incomplete source race coverage: {cell_id}')
    if set(train['race_id']) & set(evaluation['race_id']):
        raise ValueError('Training/evaluation overlap')
    train_dates = train['race_id'].str.slice(4,8) + train['race_id'].str.slice(9,13)
    cutoff = str(case['cutoff']).replace('-','')
    if (train_dates>=cutoff).any():
        raise ValueError('Training labels reached fold cutoff')
    target = OUT/cell_id/'year-2023'
    x_train = np.asarray(train.loc[:,FEATURES].to_numpy(),dtype=np.float32)
    x_evaluation = np.asarray(evaluation.loc[:,FEATURES].to_numpy(),dtype=np.float32)
    relevance_for_top5(np.asarray(evaluation['actual_finish'],dtype=np.float32),np.asarray(evaluation['abnormality_code'],dtype=np.str_))
    print('FIT_START',cell_id,len(train),'rows',len(evaluation),'evaluation rows',flush=True)
    scores = fit_and_predict(x_train=x_train,finishes=np.asarray(train['actual_finish'],dtype=np.float32),abnormality=np.asarray(train['abnormality_code'],dtype=np.str_),race_ids=np.asarray(train['race_id'],dtype=np.str_),x_evaluation=x_evaluation,output=target,config=CONFIG)
    reloaded = CatBoostRanker()
    reloaded.load_model(str(target/'model.json'),format='json')
    reloaded_scores = np.asarray(reloaded.predict(x_evaluation),dtype=np.float64)
    parity_error = float(np.max(np.abs(scores-reloaded_scores)))
    if not np.allclose(scores,reloaded_scores,rtol=0,atol=1e-6):
        raise ValueError('JSON export prediction parity failed')
    evaluation['model_score'] = scores
    evaluation.loc[:,['race_id','horse_id','horse_number','actual_finish','abnormality_code','tansho_odds','model_score']].to_json(target/'predictions.json',orient='records',indent=2)
    reviews = []
    for race_id, group in evaluation.groupby('race_id',sort=True):
        predicted = tuple(int(v) for v in group.sort_values(['model_score','horse_id','horse_number'],ascending=[False,True,True])['horse_number'])
        actual = {int(h):int(f) if np.isfinite(f) else None for h,f in zip(group['horse_number'],group['actual_finish'],strict=True)}
        odds = np.asarray(group['tansho_odds'],dtype=np.float64)
        market = None
        if np.isfinite(odds).all() and (odds>0).all():
            market = tuple(int(v) for v in group.sort_values(['tansho_odds','horse_number'])['horse_number'])
        reviews.append({'race_id':race_id,**asdict(review_orders(predicted=predicted,actual=actual,market=market))})
    delta = [sum(r['model_minus_market'][rank] for r in reviews if r['model_minus_market'] is not None) for rank in range(5)]
    market_complete = all(r['market_hits'] is not None for r in reviews)
    report = {'cell_id':cell_id,'year':2023,'config':asdict(CONFIG),'feature_names':FEATURES,'training_rows':len(train),'training_races':int(train['race_id'].nunique()),'evaluation_rows':len(evaluation),'evaluation_races':len(reviews),'missing_training_feature_rows':int(train['feature_row_present'].isna().sum()),'missing_evaluation_feature_rows':int(evaluation['feature_row_present'].isna().sum()),'market_complete':market_complete,'exact_market_delta':delta,'development_market_guard':bool(market_complete and delta[0]>0 and all(value>=0 for value in delta[1:])),'reviews':reviews,'json_export_max_abs_error':parity_error,'elapsed_seconds':time.monotonic()-started,'production_eligible':False}
    (target/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    SUMMARIES.append({k:v for k,v in report.items() if k not in ('reviews','feature_names')})
    print('FIT_COMPLETE',cell_id,'delta',delta,'guard',report['development_market_guard'],'seconds',report['elapsed_seconds'],flush=True)
    (OUT/'progress.json').write_text(json.dumps(SUMMARIES,indent=2),encoding='utf-8')
(OUT/'summary.json').write_text(json.dumps({'results':SUMMARIES,'production_eligible':False},indent=2),encoding='utf-8')
print('MARKETFREE_PILOT_COMPLETE',len(SUMMARIES),'cells',flush=True)
PY
