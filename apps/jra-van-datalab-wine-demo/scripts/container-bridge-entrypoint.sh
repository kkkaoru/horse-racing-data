#!/bin/sh
set -eu

data_spec=RACE
from_time=
limit=10
timeout=90
output=/data/records.txt

while [ "$#" -gt 0 ]; do
  option=$1
  shift
  if [ "$#" -eq 0 ]; then
    printf 'Missing value for %s.\n' "$option" >&2
    exit 2
  fi
  value=$1
  shift
  case "$option" in
    --data-spec) data_spec=$value ;;
    --from-time) from_time=$value ;;
    --limit) limit=$value ;;
    --timeout) timeout=$value ;;
    --output) output=$value ;;
    *)
      printf 'Unknown option: %s\n' "$option" >&2
      exit 2
      ;;
  esac
done

if [ -z "${JRA_VAN_BRIDGE_URL:-}" ] || [ -z "${JRA_VAN_BRIDGE_TOKEN:-}" ]; then
  printf 'JRA_VAN_BRIDGE_URL and JRA_VAN_BRIDGE_TOKEN are required.\n' >&2
  exit 2
fi
if [ -z "$from_time" ]; then
  printf '%s\n' '--from-time is required.' >&2
  exit 2
fi

mkdir -p "$(dirname "$output")"
temporary="$output.tmp.$$"
trap 'rm -f "$temporary"' EXIT INT TERM
curl --fail --silent --show-error --get \
  --header "Authorization: Bearer $JRA_VAN_BRIDGE_TOKEN" \
  --data-urlencode "data_spec=$data_spec" \
  --data-urlencode "from_time=$from_time" \
  --data-urlencode "limit=$limit" \
  --data-urlencode "timeout=$timeout" \
  --output "$temporary" \
  "$JRA_VAN_BRIDGE_URL/records"
mv "$temporary" "$output"
trap - EXIT INT TERM
printf 'Container bridge OK: arch=%s bytes=%s output=%s\n' \
  "$(uname -m)" "$(wc -c <"$output" | tr -d ' ')" "$output"
