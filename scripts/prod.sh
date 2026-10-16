#!/usr/bin/env bash
# Read-only production access using Access service tokens and Wrangler credentials.
set -euo pipefail

usage() {
  printf '%s\n' \
    'Usage: bun run prod <command> [arguments]' \
    '  status                    Show Wrangler authentication/account status' \
    '  login                     Sign into the viewer through Cloudflare Access' \
    '  url [/<path>]             Print the configured viewer URL' \
    '  get [/<path>]             Authenticated GET including error bodies; reject redirects' \
    '  logs [worker] [from to [limit]] Query Workers Observability events; UTC range is read-only' \
    '  queues [page]             Read one page of production Queue IDs/settings' \
    '  queue-metrics <id>        Read Cloudflare Queue backlog metrics' \
    '  container-usage <from> <to> Read bounded Container usage/runtime metrics' \
    '  browser-usage <from> <to> [--sessions] Read Browser Run metadata or bounded session details' \
    '  kv-usage <from-date> <to-date> Read KV operations by namespace and action' \
    '  d1-insights <db-id> <from> <to> Read D1 query-level timing/rows' \
    '  race-source-count <category> <YYYYMMDD> Read a safe D1 discovery count' \
    '  day-base-owner <category> <YYYYMMDD> Read the current D1 repair owner' \
    '  day-base-manifest <category> <YYYYMMDD> Read D1 day-base publication state' \
    '  workflows <name> [status] Read production Workflow instance status' \
    '  workflow-instance <name> <id> Read one Workflow instance and its errors' \
    '  worker-deployments         Read finish-position-cron deployment versions' \
    '  worker-version <uuid>      Read finish-position-cron version metadata' \
    '  prediction-readiness <YYYYMMDD> [jra|nar|ban-ei] Read scoped race completion without Containers' \
    '  kv <key>                  Read production DETAIL_SECTION_CACHE_KV' \
    '  r2 <bucket/key> <file>     Download an R2 object; never overwrite a file' \
    'get uses PC_KEIBA_VIEWER_ORIGIN and PC_KEIBA_ACCESS_CLIENT_ID/SECRET.' \
    'logs, queue, and workflows commands use CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN.'
}

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"
}

viewer_url() {
  local origin="${PC_KEIBA_VIEWER_ORIGIN:-}"
  local path="${1:-/}"
  origin="${origin%/}"
  [[ "$origin" =~ ^https://[a-zA-Z0-9][a-zA-Z0-9.-]*(:[0-9]+)?$ ]] ||
    fail 'Set PC_KEIBA_VIEWER_ORIGIN to an HTTPS origin (no credentials, path, or query).'
  [[ "$path" == /* && "$path" != //* && "$path" != *\\* && "$path" != *[$'\r\n\t']* ]] ||
    fail 'Use a site-relative path beginning with one /; URLs and control characters are rejected.'
  printf '%s%s\n' "$origin" "$path"
}

[[ "${1:-}" != -- ]] || shift
COMMAND="${1:-help}"
[[ $# -eq 0 ]] || shift
[[ "${1:-}" != -- ]] || shift
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG="$REPO/apps/pc-keiba-viewer/wrangler.jsonc"

if [[ "${PROD_SKIP_ENV_FILE:-}" != 1 ]]; then
  if [[ -f "$REPO/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$REPO/.env"
    set +a
  fi
  if [[ -f "$REPO/apps/pc-keiba-viewer/.env.local" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$REPO/apps/pc-keiba-viewer/.env.local"
    set +a
  fi
fi

case "$COMMAND" in
  help|-h|--help) usage ;;
  status)
    [[ $# -eq 0 ]] || fail 'status takes no arguments.'
    require_command bun
    exec bun x wrangler whoami
    ;;
  login)
    [[ $# -eq 0 ]] || fail 'login takes no arguments.'
    URL="$(viewer_url)"
    require_command cloudflared
    # --quiet prevents printing the JWT; cloudflared owns its native token cache.
    exec cloudflared access login --quiet "$URL"
    ;;
  url|get)
    [[ $# -le 1 ]] || fail 'url/get accepts only one site-relative path; arbitrary curl flags are not accepted.'
    URL="$(viewer_url "${1:-/}")"
    if [[ "$COMMAND" == url ]]; then
      printf '%s\n' "$URL"
      exit 0
    fi
    if [[ -n "${PC_KEIBA_ACCESS_CLIENT_ID:-}" && -n "${PC_KEIBA_ACCESS_CLIENT_SECRET:-}" ]]; then
      require_command curl
      # URL first. Never forward authentication to redirects.
      exec curl "$URL" --request GET --fail-with-body --silent --show-error \
        --location --max-redirs 0 --connect-timeout 10 --max-time 30 \
        --header "CF-Access-Client-Id: ${PC_KEIBA_ACCESS_CLIENT_ID}" \
        --header "CF-Access-Client-Secret: ${PC_KEIBA_ACCESS_CLIENT_SECRET}"
    fi
    require_command cloudflared
    exec cloudflared access curl "$URL" --request GET --fail-with-body --silent --show-error \
      --location --max-redirs 0 --connect-timeout 10 --max-time 30
    ;;
  queues)
    [[ $# -le 1 ]] || fail 'queues accepts at most one page number.'
    page="${1:-1}"
    [[ "$page" =~ ^[1-9][0-9]*$ ]] || fail 'queues page must be a positive integer.'
    account_id="${CLOUDFLARE_ACCOUNT_ID:-}"
    [[ "$account_id" =~ ^[a-zA-Z0-9]+$ ]] || fail 'Set CLOUDFLARE_ACCOUNT_ID to a Cloudflare account id.'
    token="${CLOUDFLARE_API_TOKEN:-${CLOUDFLARE_DEBUG_TOKEN:-}}"
    [[ -n "$token" ]] || fail 'Set CLOUDFLARE_API_TOKEN with Queues Read permission.'
    require_command curl
    exec curl --request GET --fail --silent --show-error \
      --connect-timeout 10 --max-time 30 \
      --header "Authorization: Bearer ${token}" \
      "https://api.cloudflare.com/client/v4/accounts/${account_id}/queues?page=${page}"
    ;;
  workflow-instance)
    [[ $# -eq 2 ]] || fail 'workflow-instance requires a Workflow name and instance ID.'
    workflow_name="$1"
    instance_id="$2"
    [[ "$workflow_name" =~ ^[a-zA-Z0-9][a-zA-Z0-9-]*$ ]] || fail 'workflow-instance requires a Workflow name of letters, digits, and hyphens.'
    [[ "$instance_id" =~ ^[a-zA-Z0-9_-]{1,100}$ ]] || fail 'workflow-instance requires an instance ID of letters, digits, hyphens, or underscores.'
    account_id="${CLOUDFLARE_ACCOUNT_ID:-}"
    [[ "$account_id" =~ ^[a-zA-Z0-9]+$ ]] || fail 'Set CLOUDFLARE_ACCOUNT_ID to a Cloudflare account id.'
    token="${CLOUDFLARE_API_TOKEN:-${CLOUDFLARE_DEBUG_TOKEN:-}}"
    [[ -n "$token" ]] || fail 'Set CLOUDFLARE_API_TOKEN with Workers Scripts Read permission.'
    require_command curl
    exec curl --request GET --fail --silent --show-error \
      --connect-timeout 10 --max-time 30 \
      --header "Authorization: Bearer ${token}" \
      "https://api.cloudflare.com/client/v4/accounts/${account_id}/workflows/${workflow_name}/instances/${instance_id}"
    ;;
  workflows)
    [[ $# -ge 1 && $# -le 2 ]] || fail 'workflows requires a Workflow name and optional status.'
    workflow_name="$1"
    [[ "$workflow_name" =~ ^[a-zA-Z0-9][a-zA-Z0-9-]*$ ]] || fail 'workflows requires a Workflow name of letters, digits, and hyphens.'
    workflow_status="${2:-}"
    if [[ -n "$workflow_status" ]]; then
      case "$workflow_status" in
        queued|running|paused|errored|terminated|complete|waiting|waitingForPause|rollingBack) ;;
        *) fail 'workflows status must be a documented Workflow instance status.' ;;
      esac
    fi
    account_id="${CLOUDFLARE_ACCOUNT_ID:-}"
    [[ "$account_id" =~ ^[a-zA-Z0-9]+$ ]] || fail 'Set CLOUDFLARE_ACCOUNT_ID to a Cloudflare account id.'
    token="${CLOUDFLARE_API_TOKEN:-${CLOUDFLARE_DEBUG_TOKEN:-}}"
    [[ -n "$token" ]] || fail 'Set CLOUDFLARE_API_TOKEN with Workers Scripts Read permission.'
    require_command curl
    workflow_url="https://api.cloudflare.com/client/v4/accounts/${account_id}/workflows/${workflow_name}/instances"
    if [[ -n "$workflow_status" ]]; then
      workflow_url="${workflow_url}?status=${workflow_status}"
    fi
    exec curl --request GET --fail --silent --show-error \
      --connect-timeout 10 --max-time 30 \
      --header "Authorization: Bearer ${token}" \
      "$workflow_url"
    ;;
  prediction-readiness)
    [[ $# -ge 1 && $# -le 2 && "$1" =~ ^[0-9]{8}$ ]] || fail 'prediction-readiness requires YYYYMMDD and optional jra|nar|ban-ei.'
    if [[ $# -eq 2 && "$2" != jra && "$2" != nar && "$2" != ban-ei ]]; then
      fail 'prediction-readiness category must be jra, nar or ban-ei.'
    fi
    token="${FINISH_POSITION_CRON_TRIGGER_TOKEN:-}"
    [[ -n "$token" ]] || fail 'Set FINISH_POSITION_CRON_TRIGGER_TOKEN for the internal read-only endpoint.'
    require_command curl
    exec curl --request GET --fail --silent --show-error \
      --connect-timeout 10 --max-time 30 \
      --header "Authorization: Bearer ${token}" \
      "https://finish-position-cron.kaoru.workers.dev/api/internal/prediction-readiness?runYmd=$1${2:+&category=$2}"
    ;;
  worker-deployments|worker-version)
    if [[ "$COMMAND" == worker-deployments ]]; then
      [[ $# -eq 0 ]] || fail 'worker-deployments takes no arguments.'
      version_path=deployments
    else
      [[ $# -eq 1 && "$1" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$ ]] || fail 'worker-version requires one UUID.'
      version_path="versions/$1"
    fi
    account_id="${CLOUDFLARE_ACCOUNT_ID:-}"
    [[ "$account_id" =~ ^[a-zA-Z0-9]+$ ]] || fail 'Set CLOUDFLARE_ACCOUNT_ID to a Cloudflare account id.'
    token="${CLOUDFLARE_API_TOKEN:-${CLOUDFLARE_DEBUG_TOKEN:-}}"
    [[ -n "$token" ]] || fail 'Set CLOUDFLARE_API_TOKEN with Workers Scripts Read permission.'
    require_command curl
    exec curl --request GET --fail --silent --show-error \
      --connect-timeout 10 --max-time 30 \
      --header "Authorization: Bearer ${token}" \
      "https://api.cloudflare.com/client/v4/accounts/${account_id}/workers/scripts/finish-position-cron/${version_path}"
    ;;
  queue-metrics)
    [[ $# -eq 1 && "$1" =~ ^[a-zA-Z0-9]{1,32}$ ]] || fail 'queue-metrics requires one Queue ID (letters and digits only).'
    queue_id="$1"
    account_id="${CLOUDFLARE_ACCOUNT_ID:-}"
    [[ "$account_id" =~ ^[a-zA-Z0-9]+$ ]] || fail 'Set CLOUDFLARE_ACCOUNT_ID to a Cloudflare account id.'
    token="${CLOUDFLARE_API_TOKEN:-${CLOUDFLARE_DEBUG_TOKEN:-}}"
    [[ -n "$token" ]] || fail 'Set CLOUDFLARE_API_TOKEN with Queues Read permission.'
    require_command curl
    exec curl --request GET --fail --silent --show-error \
      --connect-timeout 10 --max-time 30 \
      --header "Authorization: Bearer ${token}" \
      "https://api.cloudflare.com/client/v4/accounts/${account_id}/queues/${queue_id}/metrics"
    ;;
  day-base-owner)
    [[ $# -eq 2 ]] || fail 'day-base-owner requires a category and YYYYMMDD date.'
    category="$1"
    run_ymd="$2"
    case "$category" in
      jra|nar|ban-ei) ;;
      *) fail 'day-base-owner category must be jra, nar, or ban-ei.' ;;
    esac
    [[ "$run_ymd" =~ ^[0-9]{8}$ ]] || fail 'day-base-owner date must use YYYYMMDD.'
    require_command bun
    sql="select category, run_ymd, requested_at, repair_generation_id from finish_position_day_base_repair_requests where category = '${category}' and run_ymd = '${run_ymd}'"
    exec bun x wrangler d1 execute finish-position-cron-db --remote \
      --config "$REPO/apps/finish-position-cron/wrangler.jsonc" \
      --command "$sql" --json
    ;;
  day-base-manifest)
    [[ $# -eq 2 ]] || fail 'day-base-manifest requires a category and YYYYMMDD date.'
    category="$1"
    run_ymd="$2"
    case "$category" in
      jra|nar|ban-ei) ;;
      *) fail 'day-base-manifest category must be jra, nar, or ban-ei.' ;;
    esac
    [[ "$run_ymd" =~ ^[0-9]{8}$ ]] || fail 'day-base-manifest date must use YYYYMMDD.'
    require_command bun
    sql="select category, run_ymd, status, repair_generation_id, object_key, object_etag, updated_at from finish_position_day_base_manifests where category = '${category}' and run_ymd = '${run_ymd}'"
    exec bun x wrangler d1 execute finish-position-cron-db --remote \
      --config "$REPO/apps/finish-position-cron/wrangler.jsonc" \
      --command "$sql" --json
    ;;
  race-source-count)
    [[ $# -eq 2 ]] || fail 'race-source-count requires a category and YYYYMMDD date.'
    category="$1"
    run_ymd="$2"
    case "$category" in
      jra) predicate="source = 'jra'" ;;
      nar) predicate="source = 'nar' and printf('%02d', cast(keibajo_code as integer)) <> '83'" ;;
      ban-ei) predicate="source = 'nar' and printf('%02d', cast(keibajo_code as integer)) = '83'" ;;
      *) fail 'race-source-count category must be jra, nar, or ban-ei.' ;;
    esac
    [[ "$run_ymd" =~ ^[0-9]{8}$ ]] || fail 'race-source-count date must use YYYYMMDD.'
    require_command bun
    sql="select count(*) as race_count from (select distinct source, printf('%02d', cast(keibajo_code as integer)) as keibajo_code, printf('%02d', cast(race_bango as integer)) as race_bango from realtime_race_sources where kaisai_nen = '${run_ymd:0:4}' and kaisai_tsukihi = '${run_ymd:4:4}' and ${predicate})"
    exec bun x wrangler d1 execute sync-realtime-data --remote \
      --config "$REPO/apps/finish-position-cron/wrangler.jsonc" \
      --command "$sql" --json
    ;;
  d1-insights)
    [[ $# -eq 3 ]] || fail 'd1-insights requires a D1 database UUID and inclusive UTC start/end dates.'
    database_id="$1"
    [[ "$database_id" =~ ^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$ ]] ||
      fail 'd1-insights requires a valid D1 database UUID.'
    account_id="${CLOUDFLARE_ACCOUNT_ID:-}"
    [[ "$account_id" =~ ^[a-zA-Z0-9]+$ ]] || fail 'Set CLOUDFLARE_ACCOUNT_ID to a Cloudflare account id.'
    token="${CLOUDFLARE_API_TOKEN:-${CLOUDFLARE_DEBUG_TOKEN:-}}"
    [[ -n "$token" ]] || fail 'Set CLOUDFLARE_API_TOKEN with Analytics Read permission.'
    require_command curl
    require_command python3
    body="$(python3 - "$account_id" "$database_id" "$2" "$3" <<'PY'
import json
import sys
from datetime import date, datetime, timedelta, timezone

MAX_QUERY_RANGE: timedelta = timedelta(days=30)
account_tag, database_id, start_raw, end_raw = sys.argv[1:5]
try:
    start = date.fromisoformat(start_raw)
    end = date.fromisoformat(end_raw)
except ValueError as exc:
    raise SystemExit("d1-insights dates must use YYYY-MM-DD.") from exc
if start.isoformat() != start_raw or end.isoformat() != end_raw:
    raise SystemExit("d1-insights dates must use YYYY-MM-DD.")
if start > end or end > datetime.now(timezone.utc).date() or end - start > MAX_QUERY_RANGE:
    raise SystemExit("d1-insights date range must be ordered, not future-dated, and at most 31 days inclusive.")
query = """query D1QueryInsights($accountTag: String!, $databaseId: String!, $start: Date!, $end: Date!) {
  viewer {
    accounts(filter: { accountTag: $accountTag }) {
      d1QueriesAdaptiveGroups(
        limit: 1000
        filter: { databaseId: $databaseId, date_geq: $start, date_leq: $end }
        orderBy: [sum_queryDurationMs_DESC]
      ) {
        dimensions { databaseId query }
        count
        avg { queryDurationMs rowsRead rowsWritten }
        sum { queryDurationMs rowsRead rowsWritten }
      }
    }
  }
}"""
print(
    json.dumps(
        {
            "query": query,
            "variables": {
                "accountTag": account_tag,
                "databaseId": database_id,
                "start": start.isoformat(),
                "end": end.isoformat(),
            },
        }
    )
)
PY
)"
    exec curl --request POST --fail --silent --show-error \
      --connect-timeout 10 --max-time 30 \
      --header "Authorization: Bearer ${token}" \
      --header "Content-Type: application/json" \
      --data "$body" \
      "https://api.cloudflare.com/client/v4/graphql"
    ;;
  container-usage)
    [[ $# -eq 2 ]] || fail 'container-usage requires UTC start and end timestamps.'
    account_id="${CLOUDFLARE_ACCOUNT_ID:-}"
    [[ "$account_id" =~ ^[a-zA-Z0-9]+$ ]] || fail 'Set CLOUDFLARE_ACCOUNT_ID to a Cloudflare account id.'
    token="${CLOUDFLARE_API_TOKEN:-${CLOUDFLARE_DEBUG_TOKEN:-}}"
    [[ -n "$token" ]] || fail 'Set CLOUDFLARE_API_TOKEN with Analytics Read permission.'
    require_command curl
    require_command python3
    body="$(python3 - "$account_id" "$@" <<'PY'
import json
import sys
from datetime import datetime, timedelta, timezone

MAX_QUERY_RANGE: timedelta = timedelta(days=7)
account_tag, start_raw, end_raw = sys.argv[1:4]
try:
    start = datetime.fromisoformat(start_raw.replace("Z", "+00:00"))
    end = datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
except ValueError as exc:
    raise SystemExit("container-usage timestamps must be valid ISO-8601 values.") from exc
if start.utcoffset() != timedelta(0) or end.utcoffset() != timedelta(0):
    raise SystemExit("container-usage timestamps must use UTC (Z or +00:00).")
start = start.astimezone(timezone.utc)
end = end.astimezone(timezone.utc)
if start >= end or end > datetime.now(timezone.utc) or end - start > MAX_QUERY_RANGE:
    raise SystemExit("container-usage UTC range must be ordered, not future-dated, and at most 7 days.")
query = """query ContainerRuntime($accountTag: String, $datetimeStart: Time, $datetimeEnd: Time) {
  viewer {
    accounts(filter: { accountTag: $accountTag }) {
      containersMetricsAdaptiveGroups(
        limit: 100
        filter: { datetime_geq: $datetimeStart, datetime_leq: $datetimeEnd }
        orderBy: [datetimeHour_ASC]
      ) {
        dimensions { applicationId datetimeHour }
        sum { containerUptime cpuTimeSec allocatedMemory allocatedDisk }
        max { memory diskUsage }
      }
      containersUsageAdaptiveGroups(
        limit: 100
        filter: { datetime_geq: $datetimeStart, datetime_leq: $datetimeEnd }
        orderBy: [datetimeHour_ASC]
      ) {
        dimensions { applicationId datetimeHour }
        sum { cpuTimeSec allocatedMemory allocatedDisk }
      }
    }
  }
}"""
print(
    json.dumps(
        {
            "query": query,
            "variables": {
                "accountTag": account_tag,
                "datetimeStart": start.isoformat().replace("+00:00", "Z"),
                "datetimeEnd": end.isoformat().replace("+00:00", "Z"),
            },
        }
    )
)
PY
)"
    exec curl --request POST --fail --silent --show-error \
      --connect-timeout 10 --max-time 30 \
      --header "Authorization: Bearer ${token}" \
      --header "Content-Type: application/json" \
      --data "$body" \
      "https://api.cloudflare.com/client/v4/graphql"
    ;;
  browser-usage)
    [[ $# -eq 2 || ( $# -eq 3 && "${3:-}" == '--sessions' ) ]] || fail 'browser-usage requires UTC start and end timestamps (optional --sessions).'
    account_id="${CLOUDFLARE_ACCOUNT_ID:-}"
    [[ "$account_id" =~ ^[a-zA-Z0-9]+$ ]] || fail 'Set CLOUDFLARE_ACCOUNT_ID to a Cloudflare account id.'
    token="${CLOUDFLARE_API_TOKEN:-${CLOUDFLARE_DEBUG_TOKEN:-}}"
    [[ -n "$token" ]] || fail 'Set CLOUDFLARE_API_TOKEN with Analytics Read permission.'
    require_command curl
    require_command python3
    body="$(python3 - "$account_id" "$@" <<'PY'
import json
import sys
from datetime import datetime, timedelta, timezone

account_tag, start_raw, end_raw = sys.argv[1:4]
session_details = len(sys.argv) == 5
try:
    start = datetime.fromisoformat(start_raw.replace("Z", "+00:00"))
    end = datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
except ValueError as exc:
    raise SystemExit("browser-usage timestamps must be valid ISO-8601 values.") from exc
if start.utcoffset() != timedelta(0) or end.utcoffset() != timedelta(0):
    raise SystemExit("browser-usage timestamps must use UTC (Z or +00:00).")
start = start.astimezone(timezone.utc)
end = end.astimezone(timezone.utc)
max_range = timedelta(hours=1) if session_details else timedelta(days=1)
if start >= end or end > datetime.now(timezone.utc) or end - start > max_range:
    raise SystemExit("browser-usage UTC range must be ordered, not future-dated, and at most 1 hour with --sessions (otherwise 1 day).")
query = """query BrowserUsage($accountTag: String, $datetimeStart: Time, $datetimeEnd: Time) {
  viewer {
    accounts(filter: { accountTag: $accountTag }) {
      browserRenderingEventsAdaptiveGroups(
        limit: 50
        filter: { datetime_geq: $datetimeStart, datetime_lt: $datetimeEnd }
      ) {
        dimensions { scriptName clientLibrary browserCloseReason }
        count
      }
      browserRenderingBrowserTimeUsageAdaptiveGroups(
        limit: 1
        filter: { datetime_geq: $datetimeStart, datetime_lt: $datetimeEnd }
      ) {
        sum { totalSessionDurationMs }
      }
    }
  }
}"""
if session_details:
    query = """query BrowserSessions($accountTag: String, $datetimeStart: Time, $datetimeEnd: Time) {
  viewer {
    accounts(filter: { accountTag: $accountTag }) {
      browserRenderingEventsAdaptiveGroups(
        limit: 10000
        filter: { datetime_geq: $datetimeStart, datetime_lt: $datetimeEnd }
      ) {
        dimensions { sessionId clientLibrary browserCloseReason }
        count
      }
      browserRenderingBrowserTimeUsageAdaptiveGroups(
        limit: 5000
        filter: { datetime_geq: $datetimeStart, datetime_lt: $datetimeEnd }
      ) {
        dimensions { sessionId }
        sum { totalSessionDurationMs }
      }
    }
  }
}"""
print(json.dumps({"query": query, "variables": {
    "accountTag": account_tag,
    "datetimeStart": start.isoformat().replace("+00:00", "Z"),
    "datetimeEnd": end.isoformat().replace("+00:00", "Z"),
}}))
PY
)"
    exec curl --request POST --fail --silent --show-error \
      --connect-timeout 10 --max-time 30 \
      --header "Authorization: Bearer ${token}" \
      --header "Content-Type: application/json" \
      --data "$body" \
      "https://api.cloudflare.com/client/v4/graphql"
    ;;
  kv-usage)
    [[ $# -eq 2 ]] || fail 'kv-usage requires inclusive UTC start and exclusive end dates (YYYY-MM-DD).'
    account_id="${CLOUDFLARE_ACCOUNT_ID:-}"
    [[ "$account_id" =~ ^[a-zA-Z0-9]+$ ]] || fail 'Set CLOUDFLARE_ACCOUNT_ID to a Cloudflare account id.'
    token="${CLOUDFLARE_API_TOKEN:-${CLOUDFLARE_DEBUG_TOKEN:-}}"
    [[ -n "$token" ]] || fail 'Set CLOUDFLARE_API_TOKEN with Analytics Read permission.'
    require_command curl
    require_command python3
    body="$(python3 - "$account_id" "$@" <<'PY'
import json
import sys
from datetime import date, datetime, timedelta, timezone

account_tag, start_raw, end_raw = sys.argv[1:4]
try:
    start = date.fromisoformat(start_raw)
    end = date.fromisoformat(end_raw)
except ValueError as exc:
    raise SystemExit("kv-usage dates must use YYYY-MM-DD.") from exc
if start.isoformat() != start_raw or end.isoformat() != end_raw:
    raise SystemExit("kv-usage dates must use YYYY-MM-DD.")
if start >= end or end > datetime.now(timezone.utc).date() or end - start > timedelta(days=7):
    raise SystemExit("kv-usage date range must be ordered, completed, and at most 7 days.")
query = """query KvUsage($accountTag: String, $datetimeStart: Time, $datetimeEnd: Time) {
  viewer {
    accounts(filter: { accountTag: $accountTag }) {
      kvOperationsAdaptiveGroups(
        limit: 5000
        filter: { datetime_geq: $datetimeStart, datetime_lt: $datetimeEnd }
      ) {
        dimensions { namespaceId actionType date }
        sum { requests }
      }
    }
  }
}"""
print(json.dumps({"query": query, "variables": {
    "accountTag": account_tag,
    "datetimeStart": start.isoformat() + "T00:00:00Z",
    "datetimeEnd": end.isoformat() + "T00:00:00Z",
}}))
PY
)"
    exec curl --request POST --fail --silent --show-error \
      --connect-timeout 10 --max-time 30 \
      --header "Authorization: Bearer ${token}" \
      --header "Content-Type: application/json" \
      --data "$body" \
      "https://api.cloudflare.com/client/v4/graphql"
    ;;
  logs)
    worker="${1:-pc-keiba-viewer}"
    [[ $# -eq 0 ]] || shift
    [[ $# -eq 0 || $# -eq 2 || $# -eq 3 ]] ||
      fail 'logs accepts a Worker name and optionally a UTC start, UTC end, and event limit.'
    [[ "$worker" =~ ^[a-zA-Z0-9][a-zA-Z0-9-]*$ ]] ||
      fail 'logs requires a Worker name of letters, digits, and hyphens.'
    account_id="${CLOUDFLARE_ACCOUNT_ID:-}"
    [[ "$account_id" =~ ^[a-zA-Z0-9]+$ ]] ||
      fail 'Set CLOUDFLARE_ACCOUNT_ID to a Cloudflare account id.'
    token="${CLOUDFLARE_API_TOKEN:-${CLOUDFLARE_DEBUG_TOKEN:-}}"
    [[ -n "$token" ]] || fail 'Set CLOUDFLARE_API_TOKEN to query production Worker logs.'
    require_command curl
    require_command python3
    body="$(python3 - "$worker" "$@" <<'PY'
import json
import sys
from datetime import datetime, timedelta, timezone

DEFAULT_LOG_LIMIT: int = 20
HISTORICAL_LOG_LIMIT: int = 100
MAX_LOG_LIMIT: int = 2000
MAX_LOG_RANGE: timedelta = timedelta(days=7)
LOGS_WINDOW_MINUTES: int = 15
worker = sys.argv[1]
now = datetime.now(timezone.utc)
if len(sys.argv) == 2:
    start = now - timedelta(minutes=LOGS_WINDOW_MINUTES)
    end = now
    limit = DEFAULT_LOG_LIMIT
else:
    try:
        start = datetime.fromisoformat(sys.argv[2].replace("Z", "+00:00"))
        end = datetime.fromisoformat(sys.argv[3].replace("Z", "+00:00"))
    except ValueError as exc:
        raise SystemExit("logs UTC timestamps must be valid ISO-8601 values.") from exc
    if start.utcoffset() != timedelta(0) or end.utcoffset() != timedelta(0):
        raise SystemExit("logs timestamps must use UTC (Z or +00:00).")
    start = start.astimezone(timezone.utc)
    end = end.astimezone(timezone.utc)
    if start >= end or end > now or end - start > MAX_LOG_RANGE:
        raise SystemExit("logs UTC range must be ordered, not future-dated, and at most 7 days.")
    try:
        limit = int(sys.argv[4]) if len(sys.argv) == 5 else HISTORICAL_LOG_LIMIT
    except ValueError as exc:
        raise SystemExit("logs event limit must be an integer from 1 to 2000.") from exc
    if not 1 <= limit <= MAX_LOG_LIMIT:
        raise SystemExit("logs event limit must be an integer from 1 to 2000.")
print(
    json.dumps(
        {
            "queryId": "workers-logs-events",
            "view": "events",
            "limit": limit,
            "dry": False,
            "parameters": {
                "datasets": ["cloudflare-workers"],
                "filters": [
                    {
                        "key": "$metadata.service",
                        "operation": "eq",
                        "type": "string",
                        "value": worker,
                    }
                ],
            },
            # Workers Observability requires epoch milliseconds and retains at most seven days.
            "timeframe": {
                "from": int(start.timestamp() * 1000),
                "to": int(end.timestamp() * 1000),
            },
        }
    )
)
PY
)"
    exec curl --request POST --fail --silent --show-error \
      --connect-timeout 10 --max-time 30 \
      --header "Authorization: Bearer ${token}" \
      --header "Content-Type: application/json" \
      --data "$body" \
      "https://api.cloudflare.com/client/v4/accounts/${account_id}/workers/observability/telemetry/query"
    ;;
  kv)
    [[ $# -eq 1 && -n "$1" && "$1" != -* ]] || fail 'kv requires one nonempty key, not an option.'
    require_command bun
    exec bun x wrangler kv key get "$1" --config "$CONFIG" \
      --binding DETAIL_SECTION_CACHE_KV --remote --text
    ;;
  r2)
    [[ $# -eq 2 && "$1" == */* && "$1" != -* && -n "$2" && "$2" != -* ]] ||
      fail 'r2 requires bucket/key and a new output filename, not options.'
    [[ ! -e "$2" && ! -L "$2" ]] || fail 'Output already exists; refusing to overwrite it.'
    require_command bun
    exec bun x wrangler r2 object get "$1" --config "$CONFIG" --remote --file "$2"
    ;;
  *) usage >&2; fail "Unknown production command: $COMMAND" ;;
esac
