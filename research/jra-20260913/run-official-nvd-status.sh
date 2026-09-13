#!/usr/bin/env bash
# Verify one genuinely undefined NVD outcome against the official result table.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import hashlib
import html
import json
import re
from pathlib import Path
from urllib.request import Request, urlopen

out=Path('research/jra-20260913/official-nvd-status-001')
out.mkdir(exist_ok=False)
url='https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceMarkTable?k_raceDate=2018%2f10%2f19&k_raceNo=10&k_babaCode=27'
with urlopen(Request(url,headers={'User-Agent':'horse-racing-data-research/1.0'}),timeout=20) as response:
    raw=response.read()
    headers=dict(response.headers.items())
    final_url=response.url
(out/'result.html').write_bytes(raw)
(out/'headers.json').write_text(json.dumps(headers,indent=2),encoding='utf-8')
encoding='utf-8' if b'utf-8' in raw[:10000].lower() else 'cp932'
text=raw.decode(encoding)
(out/'result.utf8.html').write_text(text,encoding='utf-8')
rows=[]
for row in re.findall(r'<tr\b[^>]*>.*?</tr>',text,flags=re.IGNORECASE|re.DOTALL):
    if 'マイタイザン' in row:
        clean=' '.join(html.unescape(re.sub(r'<[^>]+>',' ',row)).split())
        rows.append(clean)
report={'requested_url':url,'final_url':final_url,'sha256':hashlib.sha256(raw).hexdigest(),'encoding':encoding,'matching_rows':rows,'no_source_record_modified':True}
(out/'report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
print('OFFICIAL_NVD_UNDEFINED_ROW',rows,flush=True)
PY
