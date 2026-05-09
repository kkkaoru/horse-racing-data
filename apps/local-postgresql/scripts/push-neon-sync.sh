#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

exec bun run "$APP_DIR/scripts/push-neon-sync.ts" "$@"
