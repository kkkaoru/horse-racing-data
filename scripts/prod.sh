#!/usr/bin/env bash
# Read-only production access using the existing Cloudflare CLI credentials.
set -euo pipefail

usage() {
  printf '%s\n' \
    'Usage: bun run prod <command> [arguments]' \
    '  status                    Show Wrangler authentication/account status' \
    '  login                     Sign into the viewer through Cloudflare Access' \
    '  url [/<path>]             Print the configured viewer URL' \
    '  get [/<path>]             Authenticated GET; reject redirects' \
    '  kv <key>                  Read production DETAIL_SECTION_CACHE_KV' \
    '  r2 <bucket/key> <file>     Download an R2 object; never overwrite a file' \
    'Web commands require PC_KEIBA_VIEWER_ORIGIN in the root .env or environment.'
}

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"
}

viewer_url() {
  local origin="${PC_KEIBA_VIEWER_ORIGIN:-}"
  local path="${1:-/}"
  origin="${origin%/}"
  [[ "$origin" =~ ^https://[a-zA-Z0-9][a-zA-Z0-9.-]*(:[0-9]+)?$ ]] ||
    fail 'Set PC_KEIBA_VIEWER_ORIGIN to an HTTPS origin (no credentials, path, or query).'
  [[ "$path" == /* && "$path" != //* && "$path" != *\\* && "$path" != *[$'\r\n\t']* ]] ||
    fail 'Use a site-relative path beginning with one /; URLs and control characters are rejected.'
  printf '%s%s\n' "$origin" "$path"
}

[[ "${1:-}" != -- ]] || shift
COMMAND="${1:-help}"
[[ $# -eq 0 ]] || shift
[[ "${1:-}" != -- ]] || shift
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG="$REPO/apps/pc-keiba-viewer/wrangler.jsonc"

case "$COMMAND" in
  help|-h|--help) usage ;;
  status)
    [[ $# -eq 0 ]] || fail 'status takes no arguments.'
    require_command bun
    exec bun x wrangler whoami
    ;;
  login)
    [[ $# -eq 0 ]] || fail 'login takes no arguments.'
    URL="$(viewer_url)"
    require_command cloudflared
    # --quiet prevents printing the JWT; cloudflared owns its native token cache.
    exec cloudflared access login --quiet "$URL"
    ;;
  url|get)
    [[ $# -le 1 ]] || fail 'url/get accepts only one site-relative path; arbitrary curl flags are not accepted.'
    URL="$(viewer_url "${1:-/}")"
    if [[ "$COMMAND" == url ]]; then
      printf '%s\n' "$URL"
      exit 0
    fi
    require_command cloudflared
    # URL must be the first curl argument. Never forward authentication to redirects.
    exec cloudflared access curl "$URL" --request GET --fail --silent --show-error \
      --location --max-redirs 0 --connect-timeout 10 --max-time 30
    ;;
  kv)
    [[ $# -eq 1 && -n "$1" && "$1" != -* ]] || fail 'kv requires one nonempty key, not an option.'
    require_command bun
    exec bun x wrangler kv key get "$1" --config "$CONFIG" \
      --binding DETAIL_SECTION_CACHE_KV --remote --text
    ;;
  r2)
    [[ $# -eq 2 && "$1" == */* && "$1" != -* && -n "$2" && "$2" != -* ]] ||
      fail 'r2 requires bucket/key and a new output filename, not options.'
    [[ ! -e "$2" && ! -L "$2" ]] || fail 'Output already exists; refusing to overwrite it.'
    require_command bun
    exec bun x wrangler r2 object get "$1" --config "$CONFIG" --remote --file "$2"
    ;;
  *) usage >&2; fail "Unknown production command: $COMMAND" ;;
esac
