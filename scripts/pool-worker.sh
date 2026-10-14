#!/usr/bin/env bash
# Runs only within the single Slurm allocation, never on the login node.
set -euo pipefail

pool_dir=''
runner_script=''
runner_archive=''
state_dir=''
while [[ $# -gt 0 ]]; do
  case "$1" in
    --pool-dir) pool_dir="$2"; shift 2 ;;
    --runner-script) runner_script="$2"; shift 2 ;;
    --runner-archive) runner_archive="$2"; shift 2 ;;
    --state-dir) state_dir="$2"; shift 2 ;;
    *) echo "unknown pool argument: $1" >&2; exit 2 ;;
  esac
done

: "${SLURM_JOB_ID:?pool worker must run under Slurm}"
[[ -n "$pool_dir" && -n "$state_dir" && -x "$runner_script" && -r "$runner_archive" ]]

request_dir="$pool_dir/requests"
mkdir -p "$request_dir"
chmod 700 "$pool_dir" "$request_dir"
active_file=''
child_pid=''

cleanup() {
  trap - TERM INT
  if [[ -n "$child_pid" ]]; then
    kill -TERM "$child_pid" 2>/dev/null || true
    wait "$child_pid" 2>/dev/null || true
  fi
  if [[ -n "$active_file" ]]; then
    rm -f -- "$active_file"
  fi
  exit 0
}
trap cleanup TERM INT

# A prior allocation may have been killed at its time limit. A runner that
# was interrupted must not be retried using an old GitHub JIT identity.
for abandoned in "$request_dir"/*.running; do
  [[ -f "$abandoned" ]] || continue
  echo "$(date -Is) dropping interrupted runner marker: $(basename "$abandoned")"
  rm -f -- "$abandoned"
done

echo "$(date -Is) reusable runner pool online slurm_job=$SLURM_JOB_ID"
# Reserve time for the longest (30-minute) CI run and cleanup before starting.
# Query only when work arrives, not on each idle poll.
enough_time_left() {
  local left
  left="$(squeue -h -j "$SLURM_JOB_ID" -o '%L' 2>/dev/null)" || return 1
  if [[ "$left" =~ ^([0-9]+):([0-9]{2}):([0-9]{2})$ ]]; then
    local hrs="${BASH_REMATCH[1]}" mins="${BASH_REMATCH[2]}" secs="${BASH_REMATCH[3]}"
    (( 10#$hrs * 3600 + 10#$mins * 60 + 10#$secs >= 2100 ))
    return $?
  fi
  return 1
}
while :; do
  for request in "$request_dir"/*.req; do
    [[ -f "$request" ]] || continue
    name="$(basename "$request" .req)"
    if [[ ! "$name" =~ ^c2ln1-[0-9a-f]{8}$ ]]; then
      echo "$(date -Is) ignoring unexpected runner name" >&2
      continue
    fi
    if ! enough_time_left; then
      echo "$(date -Is) pool is near its deadline or cannot verify remaining time; draining"
      exit 0
    fi
    active_file="$request_dir/$name.running"
    mv -- "$request" "$active_file"
    jit_file="$state_dir/jit/$name.jit"
    echo "$(date -Is) starting ephemeral runner $name"
    "$runner_script" --runner-name "$name" --jit-file "$jit_file" \
      --runner-archive "$runner_archive" --state-dir "$state_dir" &
    child_pid=$!
    if wait "$child_pid"; then
      echo "$(date -Is) runner exited successfully $name"
    else
      echo "$(date -Is) runner exited unsuccessfully $name" >&2
    fi
    child_pid=''
    rm -f -- "$active_file"
    active_file=''
  done
  sleep 2 &
  child_pid=$!
  wait "$child_pid" || true
  child_pid=''
done
