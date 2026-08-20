#!/usr/bin/env bash
set -euo pipefail
DIR=/tmp/feat-cache-seed-healthy-0816
LOG=/tmp/feat-cache-put-0816.log
: >"$LOG"
ok=0
fail=0
skip=0
for f in "$DIR"/*.parquet; do
  base=$(basename "$f" .parquet)
  if [[ "$base" == reread-* ]]; then
    continue
  fi
  IFS=- read -r cat a b <<<"$base"
  if [[ "$cat" == "ban" ]]; then
    # ban-ei-83-01
    cat="ban-ei"
    rest=${base#ban-ei-}
    keibajo=${rest%%-*}
    bango=${rest#*-}
  else
    keibajo=$a
    bango=$b
  fi
  key="feat-cache/catalog-v1/${cat}/20260816/${keibajo}/${bango}/features.parquet"
  if bunx wrangler r2 object put \
    "pc-keiba-features-archive/${key}" \
    --remote \
    --file "$f" \
    --content-type application/octet-stream \
    --force >>"$LOG" 2>&1; then
    ok=$((ok + 1))
    echo "OK $key"
  else
    fail=$((fail + 1))
    echo "FAIL $key"
  fi
done
echo "DONE ok=$ok fail=$fail skip=$skip" | tee -a "$LOG"
