#!/usr/bin/env bash
# Exercise real canonical historical clock/weight builders on eight JRA development fields.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
set -a
# Local credentials are runtime configuration, not a checked-in shell module.
# shellcheck source=/dev/null
source apps/local-postgresql/.env
set +a
export PGHOST=127.0.0.1 PGPORT="${POSTGRES_PORT:?}" PGUSER="${POSTGRES_USER:?}"
export PGPASSWORD="${POSTGRES_PASSWORD:?}" PGDATABASE="${POSTGRES_DB:?}" PGCONNECT_TIMEOUT=10
export PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=90000'
export PYTHONPATH="$PWD/apps/pc-keiba-viewer/src/scripts/finish-position-features:${PYTHONPATH:-}"
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import gzip
import hashlib
import json
import runpy
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import psycopg
import pyarrow.dataset as ds

root=Path('research/jra-20260913')
out=root/'jra-physics-contract-001'
out.mkdir(exist_ok=False)
feature_dir=Path('apps/pc-keiba-viewer/src/scripts/finish-position-features')
section=runpy.run_path(str(feature_dir/'add-sectional-and-weight-features.py'))
relation=runpy.run_path(str(feature_dir/'add-relationship-r1-features.py'))
functions={name:section[name] for name in ('stage_history','stage_horse_history_lookup','stage_horse_history_agg')}
functions.update({name:relation[name] for name in ('stage_race_history','stage_history_normalized')})
if not all(callable(value) for value in functions.values()):
    raise TypeError('Canonical physics builders unavailable')
section_names=['recent_soha_time_per_meter_avg5','same_distance_soha_time_per_meter_avg5','bataiju_avg5','weight_trend_5','weight_volatility_5']
relation_names=['past_speed_kg_normalized_avg5','past_speed_futan_normalized_avg5','past_speed_age_adjusted_avg5','past_speed_volatility_5','past_finish_position_volatility_5']
names=section_names+relation_names
races=json.loads((root/'jra-margin-contract-002/report.json').read_text(encoding='utf-8'))['races']
source=Path('/Users/kkk4oru/.local/share/horse-racing-data-production/jra-cell-v1/features-2000-2026-mssd-sectional-relationship-all-history-dedup-v2')
frame=ds.dataset(source,format='parquet',partitioning='hive').to_table(columns=['race_id','ketto_toroku_bango','umaban','kyori',*names],filter=ds.field('race_id').isin(races)).to_pandas()
if frame.empty or set(frame['race_id'])!=set(races):
    raise ValueError('Development event input coverage changed')
parts=frame['race_id'].str.split(':',expand=True)
frame['source']=parts[0]
keys=['kaisai_nen','kaisai_tsukihi','keibajo_code','race_bango']
for index,key in enumerate(keys,start=1):
    frame[key]=parts[index]
frame['race_date']=frame['kaisai_nen']+frame['kaisai_tsukihi']
frame.to_parquet(out/'base.parquet',index=False)
horses=sorted(frame['ketto_toroku_bango'].unique().tolist())
with psycopg.connect('') as connection, connection.cursor() as cursor:
    cursor.execute("SELECT to_jsonb(rec) FROM race_entry_corner_features rec WHERE source='jra' AND ketto_toroku_bango=ANY(%s) AND race_date BETWEEN '20000101' AND '20231231'",(horses,))
    corner_records=[row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT to_jsonb(se) FROM jvd_se se WHERE ketto_toroku_bango=ANY(%s) AND kaisai_nen||kaisai_tsukihi BETWEEN '20000101' AND '20231231'",(horses,))
    runner_records=[row[0] for row in cursor.fetchall()]
inputs={}
for label,records in [('corner',corner_records),('runners',runner_records)]:
    path=out/f'{label}.json.gz'
    with gzip.open(path,'wt',encoding='utf-8') as stream:
        json.dump(records,stream,ensure_ascii=False)
    with path.open('rb') as stream:
        inputs[label]=hashlib.file_digest(stream,'sha256').hexdigest()
with duckdb.connect() as con:
    con.execute('SET threads=4')
    con.execute("SET memory_limit='2GB'")
    con.execute(f"SET temp_directory='{out.as_posix()}/scratch'")
    con.execute('CREATE SCHEMA pg')
    con.register('corner_input',pd.DataFrame(corner_records))
    con.register('runner_input',pd.DataFrame(runner_records))
    con.execute('CREATE TABLE pg.race_entry_corner_features AS SELECT * FROM corner_input')
    con.execute('CREATE TABLE pg.jvd_se AS SELECT * FROM runner_input')
    functions['stage_history'](con,'20000101','20231231')
    # Peer-relative clocks are NOT among these ten inputs. Do not calculate them
    # from a selected-horse subset; retain their unavailable values explicitly.
    con.execute('CREATE TEMP TABLE rec_hist_context AS SELECT *,NULL::DOUBLE AS race_relative_time_z,NULL::DOUBLE AS prior_condition_time_residual FROM rec_hist')
    functions['stage_horse_history_lookup'](con,str(out/'base.parquet'))
    functions['stage_horse_history_agg'](con)
    section_values=con.execute('SELECT * FROM horse_history_agg').fetchdf()
    con.register('base_input',frame)
    functions['stage_race_history'](con,'20000101','jra')
    functions['stage_history_normalized'](con)
    relation_values=con.execute('SELECT * FROM history_normalized').fetchdf()
identity=['source',*keys,'ketto_toroku_bango']
joined=frame.merge(section_values[identity+section_names],on=identity,suffixes=('_stored','_derived'),validate='one_to_one').merge(relation_values[identity+relation_names],on=identity,suffixes=('_stored','_derived'),validate='one_to_one')
if len(joined)!=len(frame):
    raise ValueError('Canonical physics history missing a target')
joined.to_parquet(out/'comparison.parquet',index=False)
results=[]
for name in names:
    stored=joined[name+'_stored'].to_numpy(dtype=np.float64)
    derived=joined[name+'_derived'].to_numpy(dtype=np.float64)
    finite=np.isfinite(stored)&np.isfinite(derived)
    same=np.isclose(stored,derived,rtol=1e-6,atol=1e-6,equal_nan=True)
    results.append({'feature':name,'rows':len(same),'equal_rows':int(same.sum()),'comparable_finite':int(finite.sum()),'median_absolute_error':float(np.median(np.abs(stored[finite]-derived[finite]))) if finite.any() else None})
report={'scope':'2020-2023 JRA development inputs only, no9/12numericdata','rows':len(frame),'corner_history_rows':len(corner_records),'runner_history_rows':len(runner_records),'source_hashes':inputs,'results':results,'peer_relative_clocks_not_computed_or_used':True,'clock_units':'canonical encoded_race_time_tenths_sql, not an assumed seconds conversion','production_eligible':False}
(out/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print('JRA_PHYSICS_CONTRACT',report,flush=True)
PY
