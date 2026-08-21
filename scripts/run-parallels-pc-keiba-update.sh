#!/usr/bin/env bash
# Start local PostgreSQL and a Parallels Windows VM for PC-KEIBA data update.
#
# The VM is stopped only after the guest update command reports success. A
# failed or interrupted update deliberately leaves Windows running for
# inspection instead of risking an incomplete database update.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POSTGRES_APP_DIR="$ROOT_DIR/apps/local-postgresql"
PARALLELS_VM_NAME="${PARALLELS_VM_NAME:-Windows 11}"
PARALLELS_VM_START_TIMEOUT_SEC="${PARALLELS_VM_START_TIMEOUT_SEC:-180}"
PARALLELS_VM_STOP_TIMEOUT_SEC="${PARALLELS_VM_STOP_TIMEOUT_SEC:-120}"
PC_KEIBA_UPDATE_WAIT_MINUTES="${PC_KEIBA_UPDATE_WAIT_MINUTES:-180}"
LOCAL_POSTGRES_AUTO_START="${LOCAL_POSTGRES_AUTO_START:-1}"
PARALLELS_STOP_AFTER_SUCCESS="${PARALLELS_STOP_AFTER_SUCCESS:-1}"

validate_positive_integer() {
  local name="$1"
  local value="$2"
  if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "$name must be a positive integer: $value" >&2
    return 1
  fi
}

vm_status() {
  local output
  output="$(prlctl status "$PARALLELS_VM_NAME" 2>/dev/null)" || return 1
  local status="${output##* }"
  case "$status" in
    running|stopped|suspended|paused)
      printf '%s\n' "$status"
      ;;
    *)
      echo "Unsupported Parallels VM status for '$PARALLELS_VM_NAME': $status" >&2
      return 1
      ;;
  esac
}

wait_for_vm_status() {
  local expected_status="$1"
  local timeout_sec="$2"
  local attempt=0
  while ((attempt < timeout_sec)); do
    local current_status
    current_status="$(vm_status 2>/dev/null || printf 'unknown')"
    if [[ "$current_status" == "$expected_status" ]]; then
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
  echo "Parallels VM '$PARALLELS_VM_NAME' did not become $expected_status within ${timeout_sec}s." >&2
  return 1
}

wait_for_guest_tools() {
  local timeout_sec="$1"
  local attempt=0
  while ((attempt < timeout_sec)); do
    if prlctl exec "$PARALLELS_VM_NAME" --current-user powershell.exe \
      -NoProfile -NonInteractive -Command "exit 0" >/dev/null 2>&1; then
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
  echo "Parallels Tools did not become available within ${timeout_sec}s." >&2
  return 1
}

start_windows_vm() {
  local current_status
  current_status="$(vm_status)"
  if [[ "$current_status" != "running" ]]; then
    echo "Starting Parallels VM '$PARALLELS_VM_NAME'..."
    prlctl start "$PARALLELS_VM_NAME"
    wait_for_vm_status "running" "$PARALLELS_VM_START_TIMEOUT_SEC"
  else
    echo "Parallels VM '$PARALLELS_VM_NAME' is already running."
  fi
  wait_for_guest_tools "$PARALLELS_VM_START_TIMEOUT_SEC"
}

stop_windows_vm() {
  local current_status
  current_status="$(vm_status)"
  if [[ "$current_status" == "stopped" ]]; then
    echo "Parallels VM '$PARALLELS_VM_NAME' is already stopped."
    return 0
  fi

  echo "Gracefully stopping Parallels VM '$PARALLELS_VM_NAME' to release Windows RAM..."
  prlctl stop "$PARALLELS_VM_NAME" || true
  if wait_for_vm_status "stopped" "$PARALLELS_VM_STOP_TIMEOUT_SEC"; then
    return 0
  fi

  echo "Graceful VM shutdown timed out; forcing the already-completed update VM off..." >&2
  prlctl stop "$PARALLELS_VM_NAME" --kill
  wait_for_vm_status "stopped" "$PARALLELS_VM_STOP_TIMEOUT_SEC"
}

run_guest_update() {
  local python_command="${PARALLELS_PYTHON_COMMAND:-py -3.12}"
  local guest_script="\$env:LOCALAPPDATA\\pc-keiba-auto-update\\pc_keiba_auto_update.py"
  local guest_command
  printf -v guest_command \
    '& %s "%s" --wait --wait-minutes %s --close-when-done' \
    "$python_command" "$guest_script" "$PC_KEIBA_UPDATE_WAIT_MINUTES"
  echo "Running PC-KEIBA data update in Windows VM '$PARALLELS_VM_NAME'..."
  prlctl exec "$PARALLELS_VM_NAME" --current-user powershell.exe \
    -NoProfile -NonInteractive -ExecutionPolicy Bypass \
    -Command "$guest_command"
}

cleanup() {
  local exit_code=$?
  trap - EXIT
  if [[ "$exit_code" -eq 0 && "$PARALLELS_STOP_AFTER_SUCCESS" == "1" ]]; then
    stop_windows_vm || exit_code=$?
  elif [[ "$exit_code" -ne 0 ]]; then
    echo "PC-KEIBA update did not complete; leaving the Windows VM state unchanged for inspection." >&2
  fi
  exit "$exit_code"
}

validate_positive_integer "PARALLELS_VM_START_TIMEOUT_SEC" "$PARALLELS_VM_START_TIMEOUT_SEC"
validate_positive_integer "PARALLELS_VM_STOP_TIMEOUT_SEC" "$PARALLELS_VM_STOP_TIMEOUT_SEC"
validate_positive_integer "PC_KEIBA_UPDATE_WAIT_MINUTES" "$PC_KEIBA_UPDATE_WAIT_MINUTES"

if ! command -v prlctl >/dev/null 2>&1; then
  echo "prlctl is required. Install Parallels Desktop Pro/Business and expose prlctl in PATH." >&2
  exit 1
fi

if [[ "$LOCAL_POSTGRES_AUTO_START" == "1" ]]; then
  echo "Ensuring local PostgreSQL is running..."
  bun --cwd "$POSTGRES_APP_DIR" start
fi

trap cleanup EXIT
start_windows_vm
run_guest_update
