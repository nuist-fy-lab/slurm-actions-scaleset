#!/usr/bin/env bash
# GitHub-hosted CI integration check; no Slurm allocation is submitted.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp="$(mktemp -d)"
worker_pid=''
cleanup() {
  if [[ -n "$worker_pid" ]]; then
    kill -TERM "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT
mkdir -p "$tmp/bin" "$tmp/pool/requests" "$tmp/state/jit"
printf 'archive' >"$tmp/runner.tar.gz"
cat >"$tmp/bin/squeue" <<'SCRIPT'
#!/bin/sh
echo '2-23:59:55'
SCRIPT
cat >"$tmp/bin/runner" <<'SCRIPT'
#!/bin/sh
name=''
while [ "$#" -gt 0 ]; do
  case "$1" in
    --runner-name) name="$2"; shift 2 ;;
    *) shift ;;
  esac
done
printf '%s\n' "$name" >>"$TEST_RUNNER_LOG"
SCRIPT
chmod 700 "$tmp/bin/squeue" "$tmp/bin/runner"
export PATH="$tmp/bin:$PATH" TEST_RUNNER_LOG="$tmp/runner.log" SLURM_JOB_ID=123
bash "$root/scripts/pool-worker.sh" \
  --pool-dir "$tmp/pool" --runner-script "$tmp/bin/runner" \
  --runner-archive "$tmp/runner.tar.gz" --state-dir "$tmp/state" \
  >"$tmp/pool.log" 2>&1 &
worker_pid=$!

for name in c2ln1-aaaaaaaa c2ln1-bbbbbbbb; do
  : >"$tmp/pool/requests/$name.req"
  completed=false
  for _ in $(seq 1 80); do
    if grep -qx "$name" "$tmp/runner.log" 2>/dev/null; then
      completed=true
      break
    fi
    sleep .1
  done
  if [[ "$completed" != true ]]; then
    cat "$tmp/pool.log"
    echo "pool never completed $name" >&2
    exit 1
  fi
done

[[ "$(wc -l <"$tmp/runner.log")" == 2 ]]
echo 'two CI jobs reused one live Slurm allocation successfully'
