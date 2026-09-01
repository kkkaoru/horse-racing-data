#!/usr/bin/env bash
set -euo pipefail

profile=${JRA_VAN_COLIMA_PROFILE:-jvlink-amd64}
context="colima-$profile"

if [[ $(uname -s) != Darwin || $(uname -m) != arm64 ]]; then
  printf 'This setup helper is only needed on Apple Silicon macOS.\n'
  exit 0
fi
if ! command -v colima >/dev/null 2>&1; then
  printf 'Colima is required: brew install colima docker\n' >&2
  exit 1
fi
if ! command -v qemu-img >/dev/null 2>&1; then
  printf 'QEMU is required for the x86_64 VM: brew install qemu\n' >&2
  exit 1
fi
if ! brew list lima-additional-guestagents >/dev/null 2>&1; then
  printf 'x86_64 Lima guest agents are required: brew install lima-additional-guestagents\n' >&2
  exit 1
fi

start_profile() {
  colima start "$profile" \
    --arch x86_64 \
    --vm-type qemu \
    --cpus 4 \
    --memory 6 \
    --disk 30 \
    --runtime docker \
    --activate=false
}

if ! colima status --profile "$profile" >/dev/null 2>&1; then
  if ! start_profile; then
    colima stop --profile "$profile" || true
    start_profile
  fi
fi

printf 'JV-Link x86_64 runtime is ready: %s\n' "$context"
printf 'The build and demo scripts select this context automatically.\n'
