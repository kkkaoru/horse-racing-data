#!/bin/sh
set -u

max_attempts=3
attempt=1

while :; do
  output_file="$(mktemp -t finish-position-d1-migrate.XXXXXX)"
  if bunx wrangler d1 migrations apply finish-position-cron-db --remote >"$output_file" 2>&1; then
    cat "$output_file"
    rm -f "$output_file"
    exit 0
  else
    status=$?
  fi
  cat "$output_file" >&2

  if ! grep -Eq 'code: 7403|\[code: 7403\]' "$output_file" || [ "$attempt" -ge "$max_attempts" ]; then
    rm -f "$output_file"
    exit "$status"
  fi

  rm -f "$output_file"
  delay=$((attempt * 2))
  printf 'Transient Cloudflare D1 authorization error (7403); retrying migration in %ss (%s/%s).\n' \
    "$delay" "$attempt" "$max_attempts" >&2
  sleep "$delay"
  attempt=$((attempt + 1))
done
