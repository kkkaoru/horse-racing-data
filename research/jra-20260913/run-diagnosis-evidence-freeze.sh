#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import hashlib
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

root=Path('research/jra-20260913')
out=root/'diagnosis-evidence-freeze-001'
out.mkdir(exist_ok=False)
paths=[root/name for name in (
    'cloudflare-002/predictions-20260912.json',
    'cloudflare-003/predictions-20260913.json',
    'postmortem-001/summary.json','postmortem-002/summary.json',
    'winner-pair-diagnosis-001/summary.json','winner-pair-diagnosis-001/pairs.json',
    'winner-pair-diagnosis-001/contributions.json',
    'event-fit-diagnosis-001/summary.json','event-fit-diagnosis-002/summary.json',
    'priority-event-weight-001/freeze.json','priority-event-weight-001/comparison.json',
    'jra-physics-contract-001/report.json','core-weight-contract-001/report.json',
    'incumbent-day-replay-001/report.json','incumbent-pipeline-replay-001/report.json',
    'incumbent-pipeline-replay-20260913-001/report.json',
    'incumbent-pipeline-replay-20260913-001/README.md',
    'logs/coverage-closeout-001.json','logs/closeout-checks-001.log',
)]
for relative in ('incumbent-pipeline-replay-001/report.json','incumbent-pipeline-replay-20260913-001/report.json'):
    report=json.loads((root/relative).read_text(encoding='utf-8'))
    if len(report['races'])!=24 or not all(r['all_ranks_match'] and r['version_match'] and r['roster_equal'] for r in report['races']):
        raise ValueError('Full production replay not verified')
comparison=json.loads((root/'priority-event-weight-001/comparison.json').read_text(encoding='utf-8'))
if len(comparison)!=2 or any(row['development_guard'] for row in comparison):
    raise ValueError('Weighted experiment conclusion changed')
for directory in ('historical-features-20260912-001','live-features-002'):
    paths.extend(sorted((root/directory).glob('*.parquet')))
    paths.extend(sorted((root/directory).glob('*.receipt.json')))
paths.extend(sorted((root/'cloudflare-realtime-001').glob('*.json')))
paths.extend(sorted((root/'incumbent-day-replay-001').glob('*.headers.json')))
paths.extend(root/name for name in ('event_weights.py','test_event_weights.py','run-incumbent-pipeline-replay-audit.sh','run-incumbent-pipeline-replay-20260913.sh'))
items=[]
for path in paths:
    with path.open('rb') as stream:
        digest=hashlib.file_digest(stream,'sha256').hexdigest()
    items.append({'path':str(path),'bytes':path.stat().st_size,'sha256':digest})
manifest={'created_at':datetime.now(ZoneInfo('Asia/Tokyo')).isoformat(),'deadline':'2026-09-13T09:00:00+09:00','scope':'freeze of retained evidence at this time; not proof of original served model byte attestation','files':items,'full_rank_and_version_replay':{'20260912':24,'20260913':24},'20260913_outcomes_evaluated':False,'new_production_model_adopted':False,'weighted_intervention_rejected':True,'unresolved':['historical rich incumbent inputs for2020-2023','full point-in-time and original served byte provenance','three foreign JVD incomplete peer rosters','remaining20 NVD feature columns','single causal explanation and sufficient independent target-event evaluation']}
(out/'manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
print('DIAGNOSIS_EVIDENCE_FROZEN',len(items),sum(item['bytes'] for item in items),flush=True)
PY
