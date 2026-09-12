#!/usr/bin/env bash
# Cloudflare raceResults, not the viewer's historical-results section, is authoritative.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
OUT="$ROOT/research/jra-20260913/cloudflare-realtime-001"
mkdir "$OUT"
for venue in 06 09; do
  for race in 01 02 03 04 05 06 07 08 09 10 11 12; do
    curl --disable --fail --connect-timeout 5 --max-time 20 --retry 1 --retry-max-time 30 \
      --silent --show-error --dump-header "$OUT/20260912-$venue-$race.headers" \
      --output "$OUT/20260912-$venue-$race.json" \
      "https://sync-realtime-data.kkk4oru.com/api/jra/races/2026/09/12/$venue/$race/realtime"
    printf 'CLOUDFLARE_REALTIME_SAVED 20260912-%s-%s\n' "$venue" "$race"
  done
done
bash research/jra-20260913/run-viewer-read.sh --tool get_daily_finish_predictions \
  --arguments '{"year":"2026","month":"09","day":"13","source":"jra"}' \
  --complete --output research/jra-20260913/cloudflare-003/predictions-20260913.json
printf 'CLOUDFLARE_REALTIME_COLLECTION_COMPLETE\n'
