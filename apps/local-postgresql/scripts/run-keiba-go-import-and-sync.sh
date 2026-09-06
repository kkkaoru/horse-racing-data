#!/usr/bin/env bash
# Import an official NAR race day from keiba.go.jp, then publish all replicas.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VIEWER_DIR="$(cd "$APP_DIR/../pc-keiba-viewer" && pwd)"
CATALOG_DIR="$(cd "$APP_DIR/../pc-keiba-r2-catalog" && pwd)"
REPO_ENV="$(cd "$APP_DIR/../.." && pwd)/.env"

run_date=""
expect_date=0
for arg in "$@"; do
  if [[ "$expect_date" == "1" ]]; then
    run_date="$arg"
    expect_date=0
    continue
  fi
  case "$arg" in
    --date)
      expect_date=1
      ;;
    --date=*)
      run_date="${arg#--date=}"
      ;;
    --dry-run)
      echo "--dry-run cannot be combined with synchronization; use scrape:keiba-go instead." >&2
      exit 2
      ;;
  esac
done

if [[ "$expect_date" == "1" || ! "$run_date" =~ ^[0-9]{8}$ ]]; then
  echo "scrape:keiba-go-and-sync requires --date YYYYMMDD." >&2
  exit 2
fi

run_year="${run_date:0:4}"

echo "Step 1/4: importing official NAR cards for $run_date into local PostgreSQL..."
python3 "$SCRIPT_DIR/keiba_go_scrape_import.py" "$@"

echo "Step 2/4: materializing local NAR corner features for $run_date..."
bun run --cwd "$VIEWER_DIR" dev:build-corner-features -- \
  --target local \
  --source-scope nar \
  --from-date "$run_date" \
  --to-date "$run_date"

echo "Step 3/4: synchronizing local PostgreSQL to R2 Catalog and Neon..."
bun run --cwd "$APP_DIR" replica:push

echo "Step 4/4: refreshing direct-Catalog entity history for $run_year..."
bun run --env-file="$REPO_ENV" --cwd "$CATALOG_DIR" \
  sync:entity-history-serving -- --year "$run_year"

echo "keiba.go.jp import and replica synchronization completed for $run_date."
