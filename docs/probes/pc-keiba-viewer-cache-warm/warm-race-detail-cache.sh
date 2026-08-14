#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
VIEWER_ENV_FILE=${VIEWER_ENV_FILE:-$ROOT/apps/pc-keiba-viewer/.env.local}
VIEWER_ORIGIN=${VIEWER_ORIGIN:-https://pc-keiba-viewer.kkk4oru.com}
CONCURRENCY=${CONCURRENCY:-2}
EXPECTED_RACE_COUNT=${EXPECTED_RACE_COUNT:-}
OUTPUT_DIR=${OUTPUT_DIR:-$ROOT/tmp/race-detail-cache-warm}

if [[ $# -ne 1 || ! $1 =~ ^[0-9]{8}$ ]]; then
  echo "usage: $0 YYYYMMDD" >&2
  exit 2
fi
RUN_YMD=$1
YEAR=${RUN_YMD:0:4}
MONTH=${RUN_YMD:4:2}
DAY=${RUN_YMD:6:2}

if [[ ! -f $VIEWER_ENV_FILE ]]; then
  echo "viewer env file not found: $VIEWER_ENV_FILE" >&2
  exit 2
fi
set -a
# shellcheck source=/dev/null
. "$VIEWER_ENV_FILE"
set +a

: "${PC_KEIBA_ACCESS_CLIENT_ID:?missing PC_KEIBA_ACCESS_CLIENT_ID}"
: "${PC_KEIBA_ACCESS_CLIENT_SECRET:?missing PC_KEIBA_ACCESS_CLIENT_SECRET}"
command -v jq >/dev/null || { echo "jq is required" >&2; exit 2; }

mkdir -p "$OUTPUT_DIR/$RUN_YMD"
RUN_DIR=$OUTPUT_DIR/$RUN_YMD
RACES_FILE=$RUN_DIR/races.txt
RESULTS_FILE=$RUN_DIR/results.tsv
FAILURES_FILE=$RUN_DIR/failures.tsv
TIMINGS_FILE=$RUN_DIR/timings.tsv
: >"$RESULTS_FILE"
: >"$FAILURES_FILE"
printf 'race\ttarget\tphase\thttp_status\ttime_seconds\n' >"$TIMINGS_FILE"

curl_auth=(
  -H "CF-Access-Client-Id: $PC_KEIBA_ACCESS_CLIENT_ID"
  -H "CF-Access-Client-Secret: $PC_KEIBA_ACCESS_CLIENT_SECRET"
)
curl_common=(
  --fail --silent --show-error --location
  --connect-timeout 10 --max-time 180
  --retry 3 --retry-all-errors --retry-delay 5
)

DATE_URL="$VIEWER_ORIGIN/races/$YEAR/$MONTH/$DAY"
curl "${curl_common[@]}" "${curl_auth[@]}" "$DATE_URL" -o "$RUN_DIR/date-page.html"
grep -Eo "href=\"/races/$YEAR/$MONTH/$DAY/[0-9A-Z]{2}/[0-9]{2}\"" "$RUN_DIR/date-page.html" \
  | cut -d'"' -f2 \
  | sort -u >"$RACES_FILE"

race_count=$(wc -l <"$RACES_FILE" | tr -d ' ')
if [[ $race_count -eq 0 ]]; then
  echo "no race links discovered from $DATE_URL" >&2
  exit 1
fi
if [[ -n $EXPECTED_RACE_COUNT && $race_count -ne $EXPECTED_RACE_COUNT ]]; then
  echo "race count safety gate failed: expected=$EXPECTED_RACE_COUNT actual=$race_count" >&2
  exit 1
fi

echo "discovered $race_count races; checking finish-position readiness before warming"
while IFS= read -r race_path; do
  route=${race_path#"/races/"}
  payload=$RUN_DIR/preflight-${route//\//-}.json
  timing=$(curl "${curl_common[@]}" "${curl_auth[@]}" \
    "$VIEWER_ORIGIN/api/races/$route/sections/finish-prediction" -o "$payload" \
    -w '%{http_code}\t%{time_total}')
  printf '%s\tfinish-prediction\tpreflight\t%s\n' "$race_path" "$timing" >>"$TIMINGS_FILE"
  if ! jq -e '
    .type == "finish-prediction" and
    (.inputs.runners | length) > 0 and
    (.inputs.modelPredictionFeatures | length) > 0
  ' "$payload" >/dev/null; then
    printf '%s\t%s\n' "$race_path" "prediction-not-ready" >>"$FAILURES_FILE"
  fi
done <"$RACES_FILE"

if [[ -s $FAILURES_FILE ]]; then
  echo "prediction readiness safety gate failed; no non-finish sections/pages were warmed" >&2
  cat "$FAILURES_FILE" >&2
  exit 1
fi

export VIEWER_ORIGIN PC_KEIBA_ACCESS_CLIENT_ID PC_KEIBA_ACCESS_CLIENT_SECRET RUN_DIR RESULTS_FILE FAILURES_FILE TIMINGS_FILE
warm_one() {
  local race_path=$1
  local route=${race_path#"/races/"}
  local sections=(overall-score pace-prediction similar bloodline time-score premium-data-top)
  local url status timing started elapsed phase
  started=$(date +%s)
  url="$VIEWER_ORIGIN$race_path"
  for phase in warm verify; do
    timing=$(curl --silent --show-error --location --connect-timeout 10 --max-time 180 \
      --retry 3 --retry-all-errors --retry-delay 5 \
      -H "CF-Access-Client-Id: $PC_KEIBA_ACCESS_CLIENT_ID" \
      -H "CF-Access-Client-Secret: $PC_KEIBA_ACCESS_CLIENT_SECRET" \
      -o "$RUN_DIR/page-${phase}-${route//\//-}.html" -w '%{http_code}\t%{time_total}' "$url")
    status=${timing%%$'\t'*}
    printf '%s\tpage\t%s\t%s\n' "$race_path" "$phase" "$timing" >>"$TIMINGS_FILE"
    if [[ $status != 200 ]]; then
      printf '%s\tpage-%s\t%s\n' "$race_path" "$phase" "$status" >>"$FAILURES_FILE"
      return 1
    fi
  done
  for section in "${sections[@]}"; do
    url="$VIEWER_ORIGIN/api/races/$route/sections/$section"
    for phase in warm verify; do
      timing=$(curl --silent --show-error --location --connect-timeout 10 --max-time 180 \
        --retry 3 --retry-all-errors --retry-delay 5 \
        -H "CF-Access-Client-Id: $PC_KEIBA_ACCESS_CLIENT_ID" \
        -H "CF-Access-Client-Secret: $PC_KEIBA_ACCESS_CLIENT_SECRET" \
        -o "$RUN_DIR/${section}-${phase}-${route//\//-}.json" -w '%{http_code}\t%{time_total}' "$url")
      status=${timing%%$'\t'*}
      printf '%s\t%s\t%s\t%s\n' "$race_path" "$section" "$phase" "$timing" >>"$TIMINGS_FILE"
      if [[ $status != 200 ]]; then
        printf '%s\t%s-%s\t%s\n' "$race_path" "$section" "$phase" "$status" >>"$FAILURES_FILE"
        return 1
      fi
    done
  done
  elapsed=$(( $(date +%s) - started ))
  printf '%s\tok\t%ss\n' "$race_path" "$elapsed" >>"$RESULTS_FILE"
}
export -f warm_one

xargs -P "$CONCURRENCY" -I '{}' bash -c "warm_one \"\$1\"" _ '{}' <"$RACES_FILE" || true

if [[ -s $FAILURES_FILE ]]; then
  echo "cache warm completed with failures:" >&2
  cat "$FAILURES_FILE" >&2
  exit 1
fi

completed=$(wc -l <"$RESULTS_FILE" | tr -d ' ')
if [[ $completed -ne $race_count ]]; then
  echo "completion count mismatch: discovered=$race_count completed=$completed" >&2
  exit 1
fi

awk -F '\t' '
  NR > 1 && ($3 == "warm" || $3 == "verify") {
    sum[$3] += $5; count[$3] += 1
  }
  END {
    printf "response-time average: warm=%.3fs verify=%.3fs samples=%d/%d\\n",
      sum["warm"] / count["warm"], sum["verify"] / count["verify"],
      count["warm"], count["verify"]
  }
' "$TIMINGS_FILE"
echo "cache warm passed: date=$RUN_YMD races=$completed concurrency=$CONCURRENCY results=$RESULTS_FILE timings=$TIMINGS_FILE"
