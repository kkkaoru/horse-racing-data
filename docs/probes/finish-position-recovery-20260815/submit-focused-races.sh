#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  submit-focused-races.sh --run-ymd YYYYMMDD [--execute] [--debug] [--concurrency N]
                          [--input FILE] [--log-dir DIR] [--retry-failed DIR]

Without --execute, prints the planned requests and performs no network calls.
FINISH_POSITION_CRON_TRIGGER_TOKEN must be set for --execute.
--run-ymd is required so this probe script cannot silently reuse a previous day.
EOF
}

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
input="$script_dir/races-20260815.tsv"
log_dir="$script_dir/logs/$(date -u +%Y%m%dT%H%M%SZ)"
concurrency=9
execute=false
debug=false
retry_failed_dir=""
run_ymd=""
endpoint=${FINISH_POSITION_CRON_ENDPOINT:-https://finish-position-cron.kaoru.workers.dev/api/admin/run-focused-full-race}
user_agent=${FINISH_POSITION_USER_AGENT:-horse-racing-recovery/20260815}

while (($# > 0)); do
  case "$1" in
    --execute) execute=true; shift ;;
    --debug) debug=true; shift ;;
    --concurrency) concurrency=$2; shift 2 ;;
    --input) input=$2; shift 2 ;;
    --log-dir) log_dir=$2; shift 2 ;;
    --retry-failed) retry_failed_dir=$2; shift 2 ;;
    --run-ymd) run_ymd=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ "$concurrency" =~ ^[1-9][0-9]*$ ]] || { echo "--concurrency must be a positive integer" >&2; exit 2; }
[[ "$run_ymd" =~ ^[0-9]{8}$ ]] || { echo "--run-ymd YYYYMMDD is required" >&2; exit 2; }
[[ -r "$input" ]] || { echo "input is not readable: $input" >&2; exit 2; }
if $execute; then
  : "${FINISH_POSITION_CRON_TRIGGER_TOKEN:?set FINISH_POSITION_CRON_TRIGGER_TOKEN for --execute}"
fi

work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT
awk -F '\t' 'NR > 1 && NF >= 4 {print $1 "\t" $3 "\t" $4}' "$input" > "$work_dir/all.tsv"

if [[ -n "$retry_failed_dir" ]]; then
  [[ -d "$retry_failed_dir" ]] || { echo "retry log dir not found: $retry_failed_dir" >&2; exit 2; }
  find "$retry_failed_dir" -type f -name '*.jsonl' -print0 > "$work_dir/retry-files"
  [[ -s "$work_dir/retry-files" ]] || { echo "no JSONL logs found: $retry_failed_dir" >&2; exit 2; }
  xargs -0 jq -s -r '
      sort_by(.finishedAt) | group_by(.key)[] | last
      | select(.ok != true) | [.category,.keibajoCode,.raceBango] | @tsv
    ' < "$work_dir/retry-files" > "$work_dir/retry.tsv"
  awk 'NR==FNR {wanted[$0]=1; next} wanted[$0]' "$work_dir/retry.tsv" "$work_dir/all.tsv" > "$work_dir/targets.tsv"
else
  cp "$work_dir/all.tsv" "$work_dir/targets.tsv"
fi

target_count=$(wc -l < "$work_dir/targets.tsv" | tr -d ' ')
echo "targets=$target_count concurrency=$concurrency execute=$execute debug=$debug endpoint=$endpoint"
if ! $execute; then
  awk -F '\t' -v endpoint="$endpoint" -v run_ymd="$run_ymd" '{printf "POST %s category=%s runYmd=%s keibajoCode=%s raceBango=%s\n", endpoint,$1,run_ymd,$2,$3}' "$work_dir/targets.tsv"
  exit 0
fi

mkdir -p "$log_dir"
export FINISH_POSITION_CRON_TRIGGER_TOKEN endpoint user_agent log_dir debug RUN_YMD="$run_ymd"
run_one() {
  set -euo pipefail
  local category=$1 venue=$2 race=$3
  local key="${category}:${venue}:${race}"
  local started body response_file http_status curl_exit finished response response_status ok safe_key
  started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  body=$(jq -cn --arg category "$category" --arg venue "$venue" --arg race "$race" --arg runYmd "$RUN_YMD" --argjson debug "$debug" \
    '{category:$category,runYmd:$runYmd,keibajoCode:$venue,raceBango:$race,debug:$debug}')
  response_file=$(mktemp)
  trap 'rm -f "$response_file"' EXIT
  set +e
  http_status=$(curl --silent --show-error --max-time 45 --output "$response_file" --write-out "%{http_code}" \
    --request POST "$endpoint" \
    --user-agent "$user_agent" \
    --header "Authorization: Bearer $FINISH_POSITION_CRON_TRIGGER_TOKEN" \
    --header "Content-Type: application/json" \
    --data "$body")
  curl_exit=$?
  set -e
  finished=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  response=$(jq -Rs . < "$response_file")
  response_status=$(jq -r '.status // empty' "$response_file" 2>/dev/null || true)
  ok=false
  if [[ $curl_exit -eq 0 && "$http_status" =~ ^2[0-9][0-9]$ && "$response_status" != "busy" && "$response_status" != "error" ]]; then ok=true; fi
  safe_key=${key//:/-}
  jq -cn --arg key "$key" --arg category "$category" --arg venue "$venue" --arg race "$race" \
    --arg started "$started" --arg finished "$finished" --arg status "$http_status" \
    --argjson curlExit "$curl_exit" --argjson ok "$ok" --argjson response "$response" \
    '{key:$key,category:$category,keibajoCode:$venue,raceBango:$race,startedAt:$started,finishedAt:$finished,httpStatus:$status,curlExit:$curlExit,ok:$ok,response:$response}' \
    >> "$log_dir/${safe_key}.jsonl"
  printf "%s http=%s curl=%s ok=%s responseStatus=%s\n" "$key" "$http_status" "$curl_exit" "$ok" "${response_status:--}"
  [[ "$ok" == true ]]
}
export -f run_one

set +e
if ((target_count == 1)); then
  IFS=$'\t' read -r category venue race < "$work_dir/targets.tsv"
  run_one "$category" "$venue" "$race"
else
  xargs -P "$concurrency" -n 3 bash -c 'run_one "$@"' _ < "$work_dir/targets.tsv"
fi
submit_exit=$?
set -e
jq -s 'group_by(.key) | map(last) | {total:length,ok:map(select(.ok == true))|length,failed:map(select(.ok != true)|.key)}' "$log_dir"/*.jsonl > "$log_dir/summary.json"
cat "$log_dir/summary.json"
exit "$submit_exit"
