#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -f "$APP_DIR/.env.replica" ]]; then
  echo "Missing $APP_DIR/.env.replica" >&2
  exit 1
fi

set -a
# shellcheck source=/dev/null
source "$APP_DIR/.env.replica"
set +a

: "${NEON_DIRECT_DATABASE_URL:?NEON_DIRECT_DATABASE_URL is required}"
: "${LOCAL_PUBLIC_DATABASE_URL:?LOCAL_PUBLIC_DATABASE_URL is required and must be reachable from Neon}"
: "${REPLICA_PUBLICATION:=horse_racing_publication}"
: "${REPLICA_SUBSCRIPTION:=horse_racing_local_subscription}"

docker run --rm postgres:18-alpine psql "$NEON_DIRECT_DATABASE_URL" \
  -v ON_ERROR_STOP=1 \
  -v subscription="$REPLICA_SUBSCRIPTION" \
  -v publication="$REPLICA_PUBLICATION" \
  -v source_conn="$LOCAL_PUBLIC_DATABASE_URL" <<'SQL'
SELECT format(
  'CREATE SUBSCRIPTION %I CONNECTION %L PUBLICATION %I WITH (copy_data = false, create_slot = true, enabled = true)',
  :'subscription',
  :'source_conn',
  :'publication'
)
WHERE NOT EXISTS (
  SELECT 1
  FROM pg_subscription
  WHERE subname = :'subscription'
)
\gexec

SELECT subname, subenabled
FROM pg_subscription
WHERE subname = :'subscription';
SQL
