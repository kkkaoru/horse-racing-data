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

printf 'race-prediction-guard dynamic venue coverage tests passed\n'
