#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import json
from pathlib import Path

root=Path('research/jra-20260913')
output=root/'development-summary-001'
output.mkdir(exist_ok=False)
profiles=[]
for profile in ('marketfree','position'):
    years={year:json.loads((root/f'development-grid-001/{profile}/year-{year}/summary.json').read_text(encoding='utf-8'))['results'] for year in (2020,2021,2022)}
    years[2023]=json.loads((root/f'{profile}-pilot-001/summary.json').read_text(encoding='utf-8'))['results']
    cells=[]
    for cell_id in sorted({row['cell_id'] for rows in years.values() for row in rows}):
        rows=[{'year':year,**next(row for row in values if row['cell_id']==cell_id)} for year,values in years.items()]
        observed=[row for row in rows if 'exact_market_delta' in row]
        delta=[sum(row['exact_market_delta'][rank] for row in observed) for rank in range(5)]
        annual_tail_guard=all(all(v>=0 for v in row['exact_market_delta'][1:]) for row in observed)
        complete_market=bool(observed) and all(row['market_complete'] for row in observed)
        guard=complete_market and delta[0]>0 and annual_tail_guard
        cells.append({'cell_id':cell_id,'aggregate_delta':delta,'observed_years':[row['year'] for row in observed],'annual_tail_guard':annual_tail_guard,'development_guard':guard,'years':rows,'production_eligible':False})
    result={'profile':profile,'cells':cells,'fits':sum('exact_market_delta' in row for rows in years.values() for row in rows),'aggregate_delta':[sum(cell['aggregate_delta'][rank] for cell in cells) for rank in range(5)],'development_passes':[cell['cell_id'] for cell in cells if cell['development_guard']]}
    profiles.append(result)
    print(profile,'fits',result['fits'],'delta',result['aggregate_delta'],'passes',result['development_passes'],flush=True)
(output/'summary.json').write_text(json.dumps({'profiles':profiles,'later_years_used':False,'production_eligible':False},indent=2),encoding='utf-8')
PY
