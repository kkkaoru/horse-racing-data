#!/usr/bin/env bash
# Run via: bash scripts/setup-git-hooks.sh
# Configures Git 2.54+ config-based hooks for this repository.
# Required Git version: 2.54.0 or newer.

set -euo pipefail

REQUIRED_MAJOR=2
REQUIRED_MINOR=54

GIT_VERSION_RAW="$(git --version | awk '{print $3}')"
GIT_MAJOR="$(echo "$GIT_VERSION_RAW" | awk -F. '{print $1}')"
GIT_MINOR="$(echo "$GIT_VERSION_RAW" | awk -F. '{print $2}')"

if [ "$GIT_MAJOR" -lt "$REQUIRED_MAJOR" ] || { [ "$GIT_MAJOR" -eq "$REQUIRED_MAJOR" ] && [ "$GIT_MINOR" -lt "$REQUIRED_MINOR" ]; }; then
  echo "Git ${REQUIRED_MAJOR}.${REQUIRED_MINOR}+ required (config-based hooks). Found: $GIT_VERSION_RAW" >&2
  exit 1
fi

git config --local 'hook.sync-realtime-coverage.event' 'pre-push'
git config --local 'hook.sync-realtime-coverage.command' 'bun --filter sync-realtime-data run test:coverage'

echo "Configured pre-push hook(s):"
git hook list pre-push
