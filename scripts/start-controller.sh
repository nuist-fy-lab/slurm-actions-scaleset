#!/usr/bin/env bash
set -euo pipefail

config="${SLURM_ACTIONS_SCALESET_CONFIG:-$HOME/.config/slurm-actions-scaleset/controller.conf}"
if [[ ! -r "$config" ]]; then
  echo "controller config not readable: $config" >&2
  exit 1
fi
. "$config"

: "${REGISTRATION_URL:?REGISTRATION_URL is required}"
: "${SCALE_SET_NAME:?SCALE_SET_NAME is required}"
: "${MAX_RUNNERS:?MAX_RUNNERS is required}"
: "${PARTITION:?PARTITION is required}"
: "${CPUS:?CPUS is required}"
: "${MEMORY:?MEMORY is required}"
: "${TIME_LIMIT:?TIME_LIMIT is required}"
: "${RUNNER_ARCHIVE:?RUNNER_ARCHIVE is required}"
: "${RUNNER_SCRIPT:?RUNNER_SCRIPT is required}"
: "${STATE_DIR:?STATE_DIR is required}"
: "${HEARTBEAT_FILE:?HEARTBEAT_FILE is required}"

pool_args=()
if [[ "${POOL_MODE:-false}" == 'true' ]]; then
  : "${POOL_SCRIPT:?POOL_SCRIPT is required}"
  : "${POOL_TIME_LIMIT:?POOL_TIME_LIMIT is required}"
  : "${POOL_IDLE_HOURS:?POOL_IDLE_HOURS is required}"
  pool_args=(--pool-mode --pool-script "$POOL_SCRIPT" --pool-time-limit "$POOL_TIME_LIMIT" --pool-idle-hours "$POOL_IDLE_HOURS")
fi

exec "$HOME/.local/bin/slurm-actions-scaleset" \
  --url "$REGISTRATION_URL" \
  --name "$SCALE_SET_NAME" \
  --max-runners "$MAX_RUNNERS" \
  --partition "$PARTITION" \
  --cpus "$CPUS" \
  --memory "$MEMORY" \
  --time-limit "$TIME_LIMIT" \
  --state-dir "$STATE_DIR" \
  --heartbeat-file "$HEARTBEAT_FILE" \
  --runner-archive "$RUNNER_ARCHIVE" \
  --runner-script "$RUNNER_SCRIPT" \
  "${pool_args[@]}" \
  --log-format "${LOG_FORMAT:-json}"
