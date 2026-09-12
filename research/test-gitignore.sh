#!/usr/bin/env bash
# Test hypothetical paths without creating artifacts or running research jobs.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

assert_ignored() {
  if ! git check-ignore --no-index -q -- "$1"; then
    printf 'Expected generated path to be ignored: %s\n' "$1" >&2
    exit 1
  fi
}

assert_source() {
  local status=0
  git check-ignore --no-index -q -- "$1" || status=$?
  if [[ "$status" -ne 1 ]]; then
    printf 'Expected source path to be visible: %s (status %s)\n' "$1" "$status" >&2
    exit 1
  fi
}

assert_ignored research/jra-20260913/marketfree-pilot-001/cell/year-2023/model.cbm
assert_ignored research/future-campaign/new-run/model.safetensors
assert_ignored research/jra-20260913/new-run/predictions.json
assert_ignored research/future-campaign/new-run/data.parquet
assert_ignored research/future-campaign/new-run/results.csv
assert_ignored research/future-campaign/new-run/response.headers
assert_ignored research/future-campaign/new-run/.env
assert_ignored research/future-campaign/runtime/downloaded/train.py
assert_ignored research/future-campaign/.venv/lib/module.py
assert_ignored research/future-campaign/node_modules/package/index.js
assert_ignored research/future-campaign/test-work-002/generated.sh
assert_ignored research/future-campaign/test-full-002/current
assert_ignored research/future-campaign/test-model-002/generated.py

assert_source research/future-campaign/new-run/model.unknown-format
assert_source research/future-campaign/new-run/run-validation
assert_source research/future-campaign/new-run/validate.fish
assert_source research/future-campaign/new-run/analyze.rb
assert_source research/future-campaign/new-run/analyze.pl
assert_source research/future-campaign/new-run/config.json
assert_source research/jra-20260913/run-marketfree-pilot.sh
assert_source research/future-campaign/new-run/run-validation.sh
assert_source research/future-campaign/new-run/train.py
assert_source research/future-campaign/new-run/test_train.py
assert_source research/future-campaign/new-run/audit.sql
assert_source research/future-campaign/new-run/check.ts
assert_source research/future-campaign/new-run/README.md
assert_source research/future-campaign/new-run/pyproject.toml
assert_source research/future-campaign/new-run/config.yaml
assert_source research/future-campaign/new-run/package.json
assert_source research/future-campaign/new-run/requirements.txt
assert_source research/future-campaign/new-run/uv.lock
assert_source research/future-campaign/new-run/.gitignore
assert_source research/future-campaign/new-run/.env.example

# Ignore rules must not remove existing, intentionally retained snapshots.
git ls-files --error-unmatch -- research/jra-20260913/source-snapshot-001/receipt.json >/dev/null
printf 'Research ignore rules: PASS\n'
