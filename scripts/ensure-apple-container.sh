#!/usr/bin/env bash
# Ensure Apple Container CLI runtime is running.
# Use this for local PG, local predict batch, and local image rebuilds.
# Do NOT use this for wrangler Containers — that still needs Docker API via
# scripts/ensure-docker-compat.sh (colima).
set -euo pipefail

if ! command -v container >/dev/null 2>&1; then
  echo "Apple Container CLI is required. Install from https://github.com/apple/container" >&2
  exit 1
fi

if container system status >/dev/null 2>&1; then
  exit 0
fi

echo "Apple container system not running; starting..." >&2
container system start
if ! container system status >/dev/null 2>&1; then
  echo "container system start completed but status is not running" >&2
  exit 1
fi
