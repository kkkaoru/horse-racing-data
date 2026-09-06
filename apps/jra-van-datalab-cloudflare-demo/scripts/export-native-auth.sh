#!/usr/bin/env bash
set -euo pipefail

app_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
repo_root=$(cd "$app_dir/../.." && pwd)
native_app="$repo_root/apps/jra-van-datalab-wine-demo"
native_prefix="$native_app/.native-cache/prefix"
image=${JRA_VAN_CONTAINER_IMAGE:-jra-van-datalab-cloudflare-demo:local}
work_dir="$app_dir/.migration"
auth_reg="$work_dir/auth.reg"
native_identity="$native_prefix/drive_c/windows/jvsdk64.dat"
archive="$work_dir/prefix.tar.gz"
temporary_archive="$work_dir/prefix.tar.gz.tmp"

if [[ $(uname -s) != Darwin ]]; then
  printf 'Authenticated native-prefix export requires macOS.\n' >&2
  exit 1
fi
for command in bun docker python3; do
  command -v "$command" >/dev/null 2>&1 || { printf '%s is required.\n' "$command" >&2; exit 1; }
done
if [[ ! -f "$native_prefix/system.reg" || ! -f $native_identity ]]; then
  printf 'Authenticated native Wine prefix is not available at %s.\n' "$native_prefix" >&2
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

mkdir -p "$work_dir"
chmod 700 "$work_dir"
rm -f "$auth_reg" "$archive" "$temporary_archive"
python3 "$app_dir/scripts/extract_auth_registry.py" "$native_prefix/system.reg" "$auth_reg"
chmod 600 "$auth_reg"

docker run --rm --platform linux/amd64 \
  --entrypoint /bin/bash \
  --volume "$auth_reg:/run/import/auth.reg:ro" \
  --volume "$native_identity:/run/import/jvsdk64.dat:ro" \
  --volume "$work_dir:/run/export" \
  "$image" -euo pipefail -c '
    export DISPLAY=:99 WINEARCH=win64 WINEDEBUG=-all WINEPREFIX=/tmp/authenticated-prefix
    cp -a /opt/jvlink/pristine-prefix "$WINEPREFIX"
    Xvfb "$DISPLAY" -screen 0 1280x800x24 -nolisten tcp >/tmp/xvfb-import.log 2>&1 &
    xvfb_pid=$!
    trap '\''kill "$xvfb_pid" >/dev/null 2>&1 || true'\'' EXIT
    sleep 1
    wine reg import Z:\\run\\import\\auth.reg /reg:64
    cp --preserve=timestamps /run/import/jvsdk64.dat "$WINEPREFIX/drive_c/windows/jvsdk64.dat"
    chmod 600 "$WINEPREFIX/drive_c/windows/jvsdk64.dat"
    wineserver --kill >/dev/null 2>&1 || true
    tar -czf /run/export/prefix.tar.gz.tmp -C "$WINEPREFIX" \
      system.reg user.reg userdef.reg .update-timestamp drive_c/windows/jvsdk64.dat
  '
rm -f "$auth_reg"
install -m 600 "$temporary_archive" "$archive"
rm -f "$temporary_archive"
printf 'Authenticated Linux Wine state prepared at %s (secret contents not displayed).\n' "$archive"
