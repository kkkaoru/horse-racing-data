#!/usr/bin/env bash
set -euo pipefail

prefix=${WINEPREFIX:-/var/lib/jvlink/prefix}
pristine=/opt/jvlink/pristine-prefix
archive=/tmp/jvlink-prefix.tar.gz
state_url=${JRA_VAN_STATE_URL:?JRA_VAN_STATE_URL is required}
state_token=${JRA_VAN_STATE_TOKEN:?JRA_VAN_STATE_TOKEN is required}

restore() {
  rm -rf "$prefix"
  cp -a "$pristine" "$prefix"
  status=$(curl --silent --show-error --location \
    --header "Authorization: Bearer $state_token" \
    --output "$archive" --write-out '%{http_code}' "$state_url")
  if [[ $status == 200 ]]; then
    tar -xzf "$archive" -C "$prefix"
  elif [[ $status == 404 ]]; then
    :
  else
    printf 'State restore failed with HTTP %s.\n' "$status" >&2
    exit 1
  fi
  mkdir -p "$prefix/drive_c/JVData"
  rm -f "$archive"
}

checkpoint() {
  wineserver --kill >/dev/null 2>&1 || true
  state_files=(system.reg user.reg userdef.reg .update-timestamp)
  if [[ -f $prefix/drive_c/windows/jvsdk64.dat ]]; then
    state_files+=(drive_c/windows/jvsdk64.dat)
  fi
  tar -czf "$archive" -C "$prefix" "${state_files[@]}"
  wineserver --persistent
  wine net.exe start JVLink64Agent >/dev/null 2>&1 || true
  curl --fail --silent --show-error --retry 3 \
    --request PUT \
    --header "Authorization: Bearer $state_token" \
    --header 'Content-Type: application/gzip' \
    --data-binary "@$archive" "$state_url" >/dev/null
  rm -f "$archive"
}

case ${1:-} in
  restore) restore ;;
  checkpoint) checkpoint ;;
  *) printf 'Usage: state.sh restore|checkpoint\n' >&2; exit 2 ;;
esac
