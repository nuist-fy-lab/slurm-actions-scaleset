#!/usr/bin/env bash
set -euo pipefail

runner_name=''
jit_file=''
runner_archive=''

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runner-name) runner_name="$2"; shift 2 ;;
    --jit-file) jit_file="$2"; shift 2 ;;
    --runner-archive) runner_archive="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

test -n "$runner_name"
test -r "$jit_file"
test -r "$runner_archive"

export USER="${USER:-$(id -un)}"
home="$(getent passwd "$(id -u)" | cut -d: -f6)"
test -n "$home"
export HOME="$home"
toolchain="$home/.local/opt/github-runner-toolchain/bin"
lib="$home/.local/lib/github-slurm-runner"
run_dir="/tmp/github-scale-set-${SLURM_JOB_ID}-${runner_name}"
log_dir="$home/.local/state/slurm-actions-scaleset/runners/${runner_name}-${SLURM_JOB_ID}"
bridge_pid=''

cleanup() {
  mkdir -p "$log_dir"
  if [[ -d "$run_dir/_diag" ]]; then
    cp -a "$run_dir/_diag" "$log_dir/" 2>/dev/null || true
  fi
  if [[ -n "$bridge_pid" ]]; then
    kill "$bridge_pid" >/dev/null 2>&1 || true
    wait "$bridge_pid" 2>/dev/null || true
  fi
  proxy unset >/dev/null 2>&1 || true
  rm -rf "$run_dir"
}
trap cleanup EXIT INT TERM

mkdir -p "$run_dir" "$log_dir"
tar -xzf "$runner_archive" -C "$run_dir"

export PATH="$toolchain:$home/.local/bin:$PATH"
. "$home/.bash_proxy"
proxy set direct >/dev/null
git_proxy="$https_proxy"

local_proxy_host='127.0.0.1'
local_proxy_port="$((19000 + SLURM_JOB_ID % 1000))"
"$toolchain/python" "$lib/http-connect-via-socks.py" \
  --listen-host "$local_proxy_host" \
  --listen-port "$local_proxy_port" \
  --upstream "$git_proxy" >"$log_dir/http-connect-via-socks.log" 2>&1 &
bridge_pid=$!

"$toolchain/python" - "$local_proxy_host" "$local_proxy_port" <<'PY'
import socket, sys, time
host, port = sys.argv[1], int(sys.argv[2])
for _ in range(100):
    try:
        with socket.create_connection((host, port), timeout=.2):
            raise SystemExit(0)
    except OSError:
        time.sleep(.05)
raise SystemExit("local HTTP proxy did not become ready")
PY

runner_proxy="http://${local_proxy_host}:${local_proxy_port}"
export HTTP_PROXY="$runner_proxy" HTTPS_PROXY="$runner_proxy"
export http_proxy="$runner_proxy" https_proxy="$runner_proxy"
unset ALL_PROXY all_proxy FTP_PROXY ftp_proxy
export NO_PROXY='localhost,127.0.0.1,::1'
export no_proxy="$NO_PROXY"

export GIT_CONFIG_COUNT=1
export GIT_CONFIG_KEY_0='http.https://github.com.proxy'
export GIT_CONFIG_VALUE_0="$git_proxy"

export ACTIONS_RUNNER_INPUT_JITCONFIG="$(cat "$jit_file")"
rm -f "$jit_file"

cd "$run_dir"
./run.sh
