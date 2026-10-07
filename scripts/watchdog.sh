#!/usr/bin/env bash
set -euo pipefail

uid_num="$(id -u)"
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$uid_num}"
export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=$XDG_RUNTIME_DIR/bus}"

config="${SLURM_ACTIONS_SCALESET_CONFIG:-$HOME/.config/slurm-actions-scaleset/controller.conf}"
if [[ ! -r "$config" ]]; then
  echo "watchdog config not readable: $config" >&2
  exit 1
fi
. "$config"

: "${MAX_RUNNERS:?MAX_RUNNERS is required}"
: "${STATE_DIR:?STATE_DIR is required}"
: "${HEARTBEAT_FILE:?HEARTBEAT_FILE is required}"
: "${WATCHDOG_SERVICE:?WATCHDOG_SERVICE is required}"
: "${WATCHDOG_HEARTBEAT_MAX_AGE:?WATCHDOG_HEARTBEAT_MAX_AGE is required}"
: "${SLURM_JOB_PREFIX:?SLURM_JOB_PREFIX is required}"

service="$WATCHDOG_SERVICE"
state="$STATE_DIR"
heartbeat="$HEARTBEAT_FILE"
log="$state/watchdog.log"
max_runners="$MAX_RUNNERS"
heartbeat_max_age="$WATCHDOG_HEARTBEAT_MAX_AGE"
job_prefix="$SLURM_JOB_PREFIX"
mkdir -p "$state"
exec >>"$log" 2>&1

now=$(date +%s)
started=0
if ! systemctl --user is-active --quiet "$service"; then
  echo "$(date -Is) controller inactive; starting"
  systemctl --user start "$service"
  started=1
fi

if (( started == 0 )); then
  value=$(cat "$heartbeat" 2>/dev/null || echo 0)
  if [[ ! "$value" =~ ^[0-9]+$ ]] || (( now - value > heartbeat_max_age )); then
    echo "$(date -Is) heartbeat stale; restarting controller"
    systemctl --user restart "$service"
  fi
fi

mapfile -t allocations < <(
  squeue -h -u "$(id -un)" -o '%A|%j' |
    awk -F'|' -v prefix="$job_prefix" 'index($2, prefix) == 1 {print $1 "|" $2}' |
    sort -t'|' -k1,1n
)

if (( ${#allocations[@]} > max_runners )); then
  for ((i=max_runners; i<${#allocations[@]}; i++)); do
    slurm_id="${allocations[$i]%%|*}"
    echo "$(date -Is) hard cap exceeded; cancelling Slurm job=$slurm_id cap=$max_runners"
    scancel "$slurm_id" || true
  done
fi

echo "$(date -Is) healthy active_slurm_jobs=${#allocations[@]} cap=$max_runners"
