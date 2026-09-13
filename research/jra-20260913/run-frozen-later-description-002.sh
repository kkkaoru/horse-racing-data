#!/usr/bin/env bash
# Resume unstarted fits after a source-archive filename error; do not rerun completed fits.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import ast
import hashlib
import json
from pathlib import Path

root=Path('research/jra-20260913')
out=root/'frozen-later-description-002'
out.mkdir(exist_ok=False)
archive=out/'source-code'
archive.mkdir()
tasks=((2024,'position'),(2025,'marketfree'),(2025,'position'))
entries=[]
for year,profile in tasks:
    name=f'run-later-description-{profile}-{year}-002.sh'
    script=(root/f'run-later-description-{profile}-{year}-001.sh').read_text(encoding='utf-8')
    script=script.replace('frozen-later-description-001/','frozen-later-description-002/')
    script=script.replace(f'run-priority-{profile}-{year}-001.sh',name)
    ast.parse(script.split("<<'PY'\n",1)[1].rsplit('\nPY',1)[0])
    with (root/name).open('x',encoding='utf-8') as stream:
        stream.write(script)
    (archive/name).write_text(script,encoding='utf-8')
    entries.append({'year':year,'profile':profile,'path':name,'sha256':hashlib.sha256(script.encode()).hexdigest()})
(out/'continuation.json').write_text(json.dumps({'original_freeze':'frozen-later-description-001/freeze.json','reason':'classifier self-source archive name pointed at nonexistent generated path; failure occurred before fit','unchanged_learning_recipe':True,'completed_2024_marketfree_not_repeated':True,'drivers':entries,'production_eligible':False},indent=2),encoding='utf-8')
print('LATER_DESCRIPTION_CONTINUATION_FROZEN',flush=True)
PY
for task in position:2024 marketfree:2025 position:2025; do
  profile="${task%:*}"
  year="${task#*:}"
  bash "research/jra-20260913/run-later-description-${profile}-${year}-002.sh" > "research/jra-20260913/logs/later-description-${profile}-${year}-002.log" 2>&1
  printf '0\n' > "research/jra-20260913/logs/later-description-${profile}-${year}-002.exit"
  printf 'LATER_DESCRIPTION_YEAR_COMPLETE %s %s\n' "$year" "$profile"
done
