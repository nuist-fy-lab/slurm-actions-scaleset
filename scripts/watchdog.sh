#!/usr/bin/env bash
set -euo pipefail

state="$HOME/.local/state/slurm-actions-scaleset"
heartbeat="$state/heartbeat"
log="$state/watchdog.log"
mkdir -p "$state"
exec >>"$log" 2>&1

now=$(date +%s)
if ! systemctl --user is-active --quiet slurm-actions-scaleset.service; then
  echo "$(date -Is) controller inactive; starting"
  systemctl --user start slurm-actions-scaleset.service
fi

value=$(cat "$heartbeat" 2>/dev/null || echo 0)
if [[ ! "$value" =~ ^[0-9]+$ ]] || (( now - value > 120 )); then
  echo "$(date -Is) heartbeat stale; restarting controller"
  systemctl --user restart slurm-actions-scaleset.service
fi

count=$(squeue -h -u "$(id -un)" -o '%j' | grep -Ec '^ghss-c2ln1-' || true)
if (( count > 4 )); then
  echo "$(date -Is) ERROR active scale-set Slurm jobs=$count exceeds cap=4"
  exit 1
fi
echo "$(date -Is) healthy active_slurm_jobs=$count cap=4"
