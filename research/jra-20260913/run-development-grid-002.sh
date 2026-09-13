#!/usr/bin/env bash
# Bounded capacity check for the two explicitly prioritized event cells only.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
bash research/chronos2/run-local.sh "$ROOT/apps/timesfm-finish-position/.venv/bin/python" - <<'PY'
import ast
import hashlib
import json
from pathlib import Path

root = Path('research/jra-20260913')
output = root/'priority-capacity-001'
output.mkdir(exist_ok=False)
archive = output/'source-code'
archive.mkdir()
records = []
for profile in ('marketfree','position'):
    template = (root/f'run-{profile}-pilot.sh').read_text(encoding='utf-8')
    for year in (2020,2021,2022,2023):
        name = f'run-priority-{profile}-{year}-001.sh'
        script = template.replace('2023',str(year))
        script = script.replace('CONFIG = RankerConfig()','CONFIG = RankerConfig(iterations=500)')
        script = script.replace("(OUT/'cohorts.json').write_text", "CASES = CASES[:2]\n(OUT/'cohorts.json').write_text")
        script = script.replace("'maximum_fits':24", "'maximum_fits':2")
        script = script.replace(f"OUT = ROOT / '{profile}-pilot-001'",f"OUT = ROOT / 'priority-capacity-001/{profile}/year-{year}'")
        script = script.replace('OUT.mkdir(exist_ok=False)','OUT.mkdir(parents=True,exist_ok=False)')
        script = script.replace(f'run-{profile}-pilot.sh',name)
        ast.parse(script.split("<<'PY'\n",1)[1].rsplit('\nPY',1)[0])
        with (root/name).open('x',encoding='utf-8') as stream:
            stream.write(script)
        (archive/name).write_text(script,encoding='utf-8')
        records.append({'profile':profile,'year':year,'driver':name,'sha256':hashlib.sha256(script.encode()).hexdigest()})
for name in ('dedicated_cells.py','fold_ranker.py','position_classifier.py','race_rank_review.py'):
    (archive/name).write_bytes((root/name).read_bytes())
(output/'freeze.json').write_text(json.dumps({'years':[2020,2021,2022,2023],'recipes':'only capacity 100 to 500 changes; two named cells; no later-year evaluation','maximum_additional_fits':16,'drivers':records,'production_eligible':False},indent=2),encoding='utf-8')
print('DEVELOPMENT_GRID_FROZEN',len(records),'jobs',flush=True)
PY
for profile in marketfree position; do
  for year in 2020 2021 2022 2023; do
    printf 'GRID_START %s %s\n' "$profile" "$year"
    bash "research/jra-20260913/run-priority-${profile}-${year}-001.sh" > "research/jra-20260913/logs/priority-${profile}-${year}-001.log" 2>&1
    printf '0\n' > "research/jra-20260913/logs/priority-${profile}-${year}-001.exit"
    printf 'GRID_COMPLETE %s %s\n' "$profile" "$year"
  done
done
