#!/usr/bin/env bash
set -euo pipefail

app_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
repo_root=$(cd "$app_dir/../.." && pwd)
archive="$app_dir/.migration/prefix.tar.gz"
vars_file="$app_dir/.dev.vars"

for command in bun openssl; do
  command -v "$command" >/dev/null 2>&1 || { printf '%s is required.\n' "$command" >&2; exit 1; }
done
if [[ ! -f $archive ]]; then
  printf 'Run scripts/export-native-auth.sh first; %s is missing.\n' "$archive" >&2
  exit 1
fi
if ! key_valid=$(cd "$repo_root" && bun --env-file=.env -e 'const key=(process.env.JRA_VAN_DATALAB_KEY??"").replaceAll("-",""); process.stdout.write(key.length===17&&/^[A-Za-z0-9]+$/.test(key)?"1":"0")'); then
  printf 'Could not read JRA_VAN_DATALAB_KEY from the repository .env.\n' >&2
  exit 1
fi
if [[ $key_valid != 1 ]]; then
  printf 'JRA_VAN_DATALAB_KEY in .env must contain 17 alphanumeric characters, optionally hyphenated.\n' >&2
  exit 1
fi

api_token=$(openssl rand -hex 32)
state_token=$(openssl rand -hex 32)
umask 077
printf 'JRA_VAN_API_TOKEN=%s\nJRA_VAN_STATE_TOKEN=%s\n' "$api_token" "$state_token" >"$vars_file"

cd "$app_dir"
printf '%s' "$api_token" | bunx wrangler secret put JRA_VAN_API_TOKEN >/dev/null
printf '%s' "$state_token" | bunx wrangler secret put JRA_VAN_STATE_TOKEN >/dev/null
(cd "$repo_root" && bun --env-file=.env -e 'process.stdout.write((process.env.JRA_VAN_DATALAB_KEY??"").replaceAll("-",""))') \
  | bunx wrangler secret put JRA_VAN_DATALAB_KEY >/dev/null
bunx wrangler r2 object put \
  jra-van-datalab-cloudflare-state/terminal/prefix.tar.gz \
  --remote --force --content-type application/gzip --file "$archive" >/dev/null
bun run deploy
printf 'Authenticated state and secrets deployed. API tokens are stored mode 600 in %s.\n' "$vars_file"
