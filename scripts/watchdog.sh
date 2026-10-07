#!/usr/bin/env bash
set -euo pipefail

uid_num="$(id -u)"
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$uid_num}"
export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=$XDG_RUNTIME_DIR/bus}"

service="${SLURM_ACTIONS_SCALESET_SERVICE:-slurm-actions-scaleset.service}"
state="${SLURM_ACTIONS_SCALESET_STATE_DIR:-$HOME/.local/state/slurm-actions-scaleset}"
heartbeat="$state/heartbeat"
log="$state/watchdog.log"
max_runners="${SLURM_ACTIONS_SCALESET_MAX_RUNNERS:-4}"
heartbeat_max_age="${SLURM_ACTIONS_SCALESET_HEARTBEAT_MAX_AGE:-120}"
job_prefix="${SLURM_ACTIONS_SCALESET_JOB_PREFIX:-ghss-c2ln1-}"
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
