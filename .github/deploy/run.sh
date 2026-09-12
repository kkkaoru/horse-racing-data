#!/usr/bin/env bash
# Run on a GitHub-hosted Linux runner after selecting and validating main.
set -euo pipefail

target_file="$1"
validate_only="$2"
mapfile -t targets < <(python3 -c 'import json,sys; print("\n".join(json.load(open(sys.argv[1], encoding="utf-8"))))' "$target_file")

# Validate every selected service before the first production mutation.
bun run --filter '*' format:check
bun run --filter '*' lint
for target in "${targets[@]}"; do
  case "$target" in
    jra-van-datalab-worker-only-probe|umacon-worker)
      bun run --filter "$target" core:prepare
      ;;
    finish-position-cron)
      uv sync --project apps/finish-position-predict-container --frozen --python 3.12
      PYTHONPATH=apps/finish-position-predict-container/src \
        uv run --project apps/pc-keiba-viewer python .github/deploy/models.py \
          --root apps/finish-position-predict-container/models
      bun run --filter finish-position-predict-container artifact:verify -- \
        --artifact-root models --system finish-position
      bun run --filter finish-position-predict-container python:check
      ;;
  esac
  bun run --filter "$target" tsc
  bun run --filter "$target" test:coverage
done

if [[ "$validate_only" == true ]]; then
  printf '%s\n' 'Validation passed; deployment was not requested.'
  exit 0
fi

# Older queued runs must not roll production back after main advances.
current_main="$(git ls-remote origin refs/heads/main | cut -f1)"
if [[ "$current_main" != "$GITHUB_SHA" ]]; then
  printf '%s\n' 'Main advanced during verification; aborting so the newer run includes these changes.'
  exit 1
fi

for target in "${targets[@]}"; do
  printf 'Deploying %s from %s\n' "$target" "$GITHUB_SHA"
  case "$target" in
    sync-realtime-data|sync-realtime-data-hot|sync-realtime-data-features)
      bun run --filter "$target" d1:migrate
      bun run --filter "$target" deploy
      ;;
    pc-keiba-viewer)
      bun run --filter "$target" deploy:worker
      ;;
    jra-van-datalab-worker-only-probe|umacon-worker)
      bun run --filter "$target" artifact:upload
      (cd "apps/$target" && bunx wrangler deploy && bun run test:smoke)
      ;;
    *)
      bun run --filter "$target" deploy
      ;;
  esac
  # Confirm Cloudflare accepted a deployment for each service. Preserve its
  # runtime secrets; secret bulk is intentionally not part of ordinary deploys.
  (cd "apps/$target" && bunx wrangler deployments list)
done
