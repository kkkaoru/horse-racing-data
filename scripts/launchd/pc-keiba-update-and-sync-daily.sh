#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOCK_DIR="${TMPDIR:-/tmp}/com.kkk4oru.pc-keiba-update-and-sync.lock"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  printf 'PC-KEIBA update-and-sync is already running; skipping duplicate launch.\n' >&2
  exit 0
fi
trap 'rmdir "$LOCK_DIR"' EXIT HUP INT TERM

load_required_env() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    printf 'Required environment file is missing: %s\n' "$path" >&2
    exit 1
  fi
  # shellcheck disable=SC1090
  source "$path"
}

set -a
load_required_env "$REPO_DIR/.env"
load_required_env "$REPO_DIR/apps/pc-keiba-viewer/.env.local"
load_required_env "$REPO_DIR/apps/sync-realtime-data/.dev.vars"
load_required_env "$REPO_DIR/apps/local-postgresql/.env"
load_required_env "$REPO_DIR/apps/local-postgresql/.env.replica"
set +a

export LOCAL_POSTGRES_AUTO_START=1
cd "$REPO_DIR"
/opt/homebrew/bin/bun run pc-keiba:update-and-sync
