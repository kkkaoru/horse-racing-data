#!/usr/bin/env bash
# Compare frozen runner populations against declared source field sizes.
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
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import json
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg
import pyarrow.dataset as ds

root=Path('research/jra-20260913')
out=root/'source-field-completeness-001'
out.mkdir(exist_ok=False)
with psycopg.connect('') as connection:
    with connection.cursor() as cursor:
        cursor.execute("""SELECT 'jra:'||kaisai_nen||':'||kaisai_tsukihi||':'||keibajo_code||':'||race_bango AS race_id, trim(shusso_tosu), keibajo_code FROM jvd_ra WHERE kaisai_nen||kaisai_tsukihi BETWEEN '20000101' AND '20260905' AND coalesce(trim(data_kubun),'') NOT IN ('0','9')""")
        metadata=pd.DataFrame(cursor.fetchall(),columns=['race_id','declared_size_text','venue'])
        cursor.execute("""SELECT 'jra:'||kaisai_nen||':'||kaisai_tsukihi||':'||keibajo_code||':'||race_bango, trim(ketto_toroku_bango), trim(umaban), trim(bamei) FROM jvd_se WHERE kaisai_nen='2026' AND kaisai_tsukihi='0913' AND keibajo_code IN ('06','09')""")
        identities=pd.DataFrame(cursor.fetchall(),columns=['race_id','horse_id','horse_number','horse_name'])
metadata.to_json(out/'raw-field-metadata.json',orient='records',indent=2)
identities.to_json(out/'raw-upcoming-identities.json',orient='records',indent=2,force_ascii=False)
if metadata.duplicated('race_id').any():
    raise ValueError('Duplicate race revisions require explicit resolution')
status=pd.read_parquet(root/'training-status-002/jvd-runner-status.parquet')
active=status.loc[~status['abnormality_code'].isin(['1','2','3'])]
counts=active.groupby('race_id').size().rename('stored_active_rows')
comparison=metadata.merge(counts,on='race_id',how='left',validate='one_to_one')
comparison['declared_size']=pd.to_numeric(comparison['declared_size_text'],errors='coerce')
comparison['size_known']=comparison['declared_size'].gt(0)
comparison['equal']=comparison['size_known'] & comparison['declared_size'].eq(comparison['stored_active_rows'])
comparison.to_json(out/'comparison.json',orient='records',indent=2)
scopes=[]
for year in (2020,2021,2022,2023):
    cohorts=json.loads((root/f'priority-capacity-001/marketfree/year-{year}/cohorts.json').read_text(encoding='utf-8'))
    for case in cohorts:
        cohort=comparison.loc[comparison['race_id'].isin(case['training_race_ids'])]
        scopes.append({'year':year,'cell':case['cell_id'],'requested_training_races':len(case['training_race_ids']),'matched_metadata':len(cohort),'known_size':int(cohort['size_known'].sum()),'matching_size':int(cohort['equal'].sum()),'nonmatching':cohort.loc[~cohort['equal']].to_dict(orient='records')})
identity_reports=[]
predictions=json.loads((root/'cloudflare-003/predictions-20260913.json').read_text(encoding='utf-8'))
for race in predictions['races']:
    rid=race['raceId']
    source=identities.loc[identities['race_id']==rid].copy()
    source['horse_number']=pd.to_numeric(source['horse_number'],errors='raise')
    if source.duplicated('horse_number').any():
        raise ValueError('Duplicate upcoming source horse numbers')
    predicted_names={int(h['horseNumber']):''.join(unicodedata.normalize('NFKC',h['horseName']).split()) for h in race['prediction']}
    source_names={int(n):''.join(unicodedata.normalize('NFKC',name).split()) for n,name in zip(source['horse_number'],source['horse_name'],strict=True)}
    parts=rid.split(':')
    live=pd.read_parquet(root/f'live-features-002/{parts[1]}{parts[2]}-{parts[3]}-{parts[4]}.parquet')
    live_ids={int(n):str(h) for n,h in zip(live['umaban'],live['ketto_toroku_bango'],strict=True)}
    source_ids={int(n):str(h) for n,h in zip(source['horse_number'],source['horse_id'],strict=True)}
    identity_reports.append({'race_id':rid,'prediction_names_match_source':predicted_names==source_names,'live_registration_ids_match_source':live_ids==source_ids})
legacy=Path.home()/'.local/share/horse-racing-data-production/jra-cell-v1/features-2000-2026-mssd-sectional-relationship-all-history-dedup-v2'
frame=ds.dataset(legacy,format='parquet',partitioning='hive').to_table(columns=['shusso_tosu','umaban','umaban_norm']).to_pandas()
expected=np.clip((np.asarray(frame['umaban'],dtype=np.float64)-1)/(np.asarray(frame['shusso_tosu'],dtype=np.float64)-1),0,1)
equal=np.isclose(expected,np.asarray(frame['umaban_norm'],dtype=np.float64),equal_nan=True)
report={'source_scope':'frozen JVD active rows versus newly read historical JVD RA declared size; equality is not independent official-roster certification','priority_training_scopes':scopes,'upcoming_identity_linkage':identity_reports,'historical_clipped_umaban':{'rows':len(frame),'equal':int(equal.sum())},'no_roster_or_label_changes':True,'production_eligible':False}
(out/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print('SOURCE_COMPLETENESS',[(s['year'],s['cell'],s['matching_size'],s['requested_training_races']) for s in scopes],flush=True)
print('UPCOMING_IDENTITY_LINKAGE',sum(r['prediction_names_match_source'] and r['live_registration_ids_match_source'] for r in identity_reports),'/',len(identity_reports),flush=True)
print('CLIPPED_UMABAN',int(equal.sum()),'/',len(frame),flush=True)
PY
