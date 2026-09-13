#!/usr/bin/env bash
# Complete the requested time-range description without rescuing failed development arms.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import ast
import hashlib
import json
from pathlib import Path

root=Path('research/jra-20260913')
out=root/'frozen-later-description-001'
out.mkdir(exist_ok=False)
archive=out/'source-code'
archive.mkdir()
drivers=[]
for year in (2024,2025):
    for profile in ('marketfree','position'):
        name=f'run-later-description-{profile}-{year}-001.sh'
        script=(root/f'run-priority-{profile}-2023-001.sh').read_text(encoding='utf-8')
        script=script.replace('2023',str(year))
        script=script.replace(f'priority-capacity-001/{profile}/year-{year}',f'frozen-later-description-001/{profile}/year-{year}')
        script=script.replace('development_market_guard','descriptive_market_condition')
        script=script.replace('no-exact-development-races','no-matching-venue-event-races')
        script=script.replace("'production_eligible':False", "'selection_status':'development-failed-not-rescued-by-later-results','production_eligible':False")
        ast.parse(script.split("<<'PY'\n",1)[1].rsplit('\nPY',1)[0])
        with (root/name).open('x',encoding='utf-8') as stream:
            stream.write(script)
        (archive/name).write_text(script,encoding='utf-8')
        drivers.append({'year':year,'profile':profile,'path':name,'sha256':hashlib.sha256(script.encode()).hexdigest()})
for name in ('fold_ranker.py','position_classifier.py','dedicated_cells.py','race_rank_review.py'):
    (archive/name).write_bytes((root/name).read_bytes())
(out/'freeze.json').write_text(json.dumps({'purpose':'descriptive2024/2025 continuation of original500-iteration JVD-only arms, not candidate selection','development_status':'both arms already failed; later outcomes cannot change this','years':[2024,2025],'maximum_fits':8,'source_scope':'original incomplete JVD-only168-feature comparator; not NVD-complete','parameters':'unchanged500 iterations; original lexicographic classifier decoder, not newly tested joint variants','venue_scope':'no Kyoto substitution for Hanshin Challenge; missing support remains explicit','2026_status':'not included in this readout;9/13 outcome unavailable and9/12 post-hoc results cannot establish improvement','previously_observed_later_years':True,'drivers':drivers,'production_eligible':False},indent=2),encoding='utf-8')
print('LATER_DESCRIPTION_FROZEN',flush=True)
PY
for year in 2024 2025; do
  for profile in marketfree position; do
    bash "research/jra-20260913/run-later-description-${profile}-${year}-001.sh" > "research/jra-20260913/logs/later-description-${profile}-${year}-001.log" 2>&1
    printf '0\n' > "research/jra-20260913/logs/later-description-${profile}-${year}-001.exit"
    printf 'LATER_DESCRIPTION_YEAR_COMPLETE %s %s\n' "$year" "$profile"
  done
done
