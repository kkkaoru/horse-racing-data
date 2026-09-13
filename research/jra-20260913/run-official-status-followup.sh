#!/usr/bin/env bash
# Read only the two unresolved historical official statuses; retain all responses.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash research/chronos2/run-local.sh apps/timesfm-finish-position/.venv/bin/python - <<'PY'
import hashlib
import html
import json
import re
import urllib.request
from pathlib import Path

root=Path('research/jra-20260913')
out=root/'official-status-002'
out.mkdir(exist_ok=False)
records=[]
for venue,race,number,name in [('09','01','3','シュエットアムール'),('06','11','4','モンドプリューム')]:
    source=json.loads((root/f'cloudflare-realtime-001/20260912-{venue}-{race}.json').read_text(encoding='utf-8'))
    url=source['source']['debaUrl']
    request=urllib.request.Request(url,headers={'User-Agent':'horse-racing-data-research/1.0'})
    with urllib.request.urlopen(request,timeout=15) as response:
        raw=response.read()
        headers=str(response.headers)
    stem=f'20260912-{venue}-{race}'
    (out/f'{stem}-entries.html').write_bytes(raw)
    (out/f'{stem}-entries.headers').write_text(headers,encoding='utf-8')
    text=raw.decode('cp932')
    link=re.search(r'href="(/JRADB/accessS.html\?CNAME=pw01sde[^\"]+)"',text)
    if link is None:
        raise ValueError('Official result link unavailable')
    result_url='https://www.jra.go.jp'+html.unescape(link.group(1))
    request=urllib.request.Request(result_url,headers={'User-Agent':'horse-racing-data-research/1.0'})
    with urllib.request.urlopen(request,timeout=15) as response:
        raw=response.read()
        headers=str(response.headers)
    (out/f'{stem}-results.html').write_bytes(raw)
    (out/f'{stem}-results.headers').write_text(headers,encoding='utf-8')
    text=raw.decode('cp932')
    (out/f'{stem}-results.utf8.html').write_text(text,encoding='utf-8')
    location=text.find(name)
    if location<0:
        raise ValueError('Named horse absent from official results')
    row=text[text.rfind('<tr',0,location):text.find('</tr>',location)]
    visible=' '.join(html.unescape(re.sub('<[^>]+>',' ',row)).split())
    record={'race_id':source['raceKey'],'horse_number':number,'horse_name':name,'official_url':result_url,'sha256':hashlib.sha256(raw).hexdigest(),'row_text':visible,'cloudflare_numeric_finishes_unchanged':True}
    records.append(record)
    print('OFFICIAL_ROW',json.dumps(record,ensure_ascii=False),flush=True)
(out/'rows.json').write_text(json.dumps(records,ensure_ascii=False,indent=2),encoding='utf-8')
PY
