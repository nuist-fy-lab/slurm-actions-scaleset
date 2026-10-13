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
  --log-format "${LOG_FORMAT:-json}"
