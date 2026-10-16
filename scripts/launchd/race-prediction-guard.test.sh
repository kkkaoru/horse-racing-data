#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GUARD="$SCRIPT_DIR/race-prediction-guard.sh"
LOCK_DIR="/tmp/race-prediction-guard.lock"

run_guard() {
  rmdir "$LOCK_DIR" 2>/dev/null || true
  DRY_RUN=1 \
    FORCE_HOUR=05 \
    FORCE_TARGET_DATE=20300101 \
    FORCE_EXPECTED_COUNT="$1" \
    FORCE_D1_VENUE_RACES="$2" \
    FORCE_AUTHORITATIVE_VENUE_RACES="$3" \
    FORCE_NO_CORNER_FEATURES=1 \
    FORCE_RS_ACTUAL="$1" \
    FORCE_FP_ACTUAL="$1" \
    bash "$GUARD"
}

assert_contains() {
  local output="$1"
  local expected="$2"
  if ! printf '%s\n' "$output" | grep -Fq "$expected"; then
    printf 'expected output to contain: %s\n' "$expected" >&2
    exit 1
  fi
}

assert_not_contains() {
  local output="$1"
  local unexpected="$2"
  if printf '%s\n' "$output" | grep -Fq "$unexpected"; then
    printf 'expected output not to contain: %s\n' "$unexpected" >&2
    exit 1
  fi
}

nine_race_output="$(run_guard 9 \
  '46:01-02-03-04-05-06-07-08-09' \
  '46:01-02-03-04-05-06-07-08-09')"
assert_contains "$nine_race_output" "keibajo_code=46 actual=9 expected=9 missing=none"
assert_contains "$nine_race_output" "D1 covers the authoritative race card"
assert_not_contains "$nine_race_output" "discover-urls-coverage"

twelve_race_output="$(run_guard 12 \
  '44:01-02-03-04-05-06-07-08-09-10-11-12' \
  '44:01-02-03-04-05-06-07-08-09-10-11-12')"
assert_contains "$twelve_race_output" "keibajo_code=44 actual=12 expected=12 missing=none"
assert_contains "$twelve_race_output" "D1 covers the authoritative race card"
assert_not_contains "$twelve_race_output" "discover-urls-coverage"

missing_race_output="$(run_guard 11 \
  '44:01-02-03-04-05-06-07-08-09-10-12' \
  '44:01-02-03-04-05-06-07-08-09-10-11-12')"
assert_contains "$missing_race_output" "keibajo_code=44 actual=11 expected=12 missing=11"
assert_contains "$missing_race_output" "authoritative race gaps: 44(actual=11,expected=12,missing=11)"
assert_contains "$missing_race_output" "would POST https://sync-realtime-data.kkk4oru.com/api/jobs body={\"type\":\"discover-urls\",\"date\":\"20300101\"}"

mock_dir="$(mktemp -d)"
trap 'rm -rf "$mock_dir"' EXIT
printf '%s\n' '#!/usr/bin/env bash' \
  '[[ " $* " == *"/api/internal/expected-race-count?date=2030-01-01"* ]] || exit 1' \
  'case " $* " in' \
  '  *"&kind=venues"*) printf '\''{"results":[{"keibajo_code":"46","c":9,"races":"01,02,03,04,05,06,07,08,09"}]}\n'\'' ;;' \
  '  *"&kind=categories"*) printf '\''{"results":[{"category":"jra","c":9}]}\n'\'' ;;' \
  '  *) printf '\''{"count":9}\n'\'' ;;' \
  'esac' > "$mock_dir/curl"
chmod +x "$mock_dir/curl"
rmdir "$LOCK_DIR" 2>/dev/null || true
worker_count_output="$(PATH="$mock_dir:$PATH" DRY_RUN=1 FORCE_HOUR=05 FORCE_TARGET_DATE=20300101 \
  FORCE_D1_VENUE_RACES='46:01-02-03-04-05-06-07-08-09' \
  FORCE_AUTHORITATIVE_VENUE_RACES='46:01-02-03-04-05-06-07-08-09' \
  FORCE_NO_CORNER_FEATURES=1 FORCE_RS_ACTUAL=9 FORCE_FP_ACTUAL=9 bash "$GUARD")"
assert_contains "$worker_count_output" "guard_target done (label=today target=2030-01-01 expected=9"
assert_not_contains "$worker_count_output" "D1 expected-count query attempt"
rmdir "$LOCK_DIR" 2>/dev/null || true
worker_venue_output="$(PATH="$mock_dir:$PATH" DRY_RUN=1 FORCE_HOUR=05 FORCE_TARGET_DATE=20300101 \
  FORCE_AUTHORITATIVE_VENUE_RACES='46:01-02-03-04-05-06-07-08-09' \
  FORCE_NO_CORNER_FEATURES=1 FORCE_RS_ACTUAL=9 FORCE_FP_ACTUAL=9 bash "$GUARD")"
assert_contains "$worker_venue_output" "keibajo_code=46 actual=9 expected=9 missing=none"
assert_contains "$worker_venue_output" "D1 covers the authoritative race card"

printf 'race-prediction-guard dynamic venue coverage tests passed\n'
