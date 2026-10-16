#!/usr/bin/env bash
# Run with bash. No database, server or network access.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
next() {
  printf '%s:%s:%s' "$PC_KEIBA_DATABASE_TARGET" "$PC_KEIBA_ALLOW_CLOUDFLARE_DB_IN_NEXT_DEV" "$PC_KEIBA_CLOUDFLARE_REMOTE_BINDINGS"
}
export -f next
COMMAND="$(bun -e 'console.log(require("./apps/pc-keiba-viewer/package.json").scripts["dev:https"])')"
[[ "$(env -u PC_KEIBA_DATABASE_TARGET bash -c "$COMMAND")" == 'cloudflare:1:1' ]]
[[ "$(PC_KEIBA_DATABASE_TARGET=local bash -c "$COMMAND")" == 'local:1:1' ]]
[[ "$(PC_KEIBA_DATABASE_TARGET=neon bash -c "$COMMAND")" == 'neon:1:1' ]]
TARGET="$(grep '^VIEWER_DATABASE_TARGET=' scripts/start-db-and-viewer.sh)"
[[ "$(env -u PC_KEIBA_DATABASE_TARGET bash -c "$TARGET; printf '%s' \"\$VIEWER_DATABASE_TARGET\"")" == 'cloudflare' ]]
[[ "$(PC_KEIBA_DATABASE_TARGET=local bash -c "$TARGET; printf '%s' \"\$VIEWER_DATABASE_TARGET\"")" == 'local' ]]
printf 'HTTPS database selection: passed\n'
