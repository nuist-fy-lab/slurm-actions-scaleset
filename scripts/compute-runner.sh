#!/usr/bin/env bash
set -euo pipefail

runner_name=''
jit_file=''
runner_archive=''
state_dir=''

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runner-name) runner_name="$2"; shift 2 ;;
    --jit-file) jit_file="$2"; shift 2 ;;
    --runner-archive) runner_archive="$2"; shift 2 ;;
    --state-dir) state_dir="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

test -n "$runner_name"
test -r "$jit_file"
test -r "$runner_archive"
test -n "$state_dir"

export USER="${USER:-$(id -un)}"
home="$(getent passwd "$(id -u)" | cut -d: -f6)"
test -n "$home"
export HOME="$home"
toolchain="$home/.local/opt/github-runner-toolchain/bin"
lib="$home/.local/lib/slurm-actions-scaleset"
run_dir="/tmp/github-scale-set-${SLURM_JOB_ID}-${runner_name}"
log_dir="$state_dir/runners/${runner_name}-${SLURM_JOB_ID}"
toolchain_lock="$home/.local/state/slurm-actions-scaleset/toolchain.lock"
job_git_config="$run_dir/gitconfig"
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

mkdir -p "$run_dir" "$log_dir" "$(dirname "$toolchain_lock")"

# Runner jobs may share one read-only toolchain, while maintenance takes an
# exclusive lock before mutating it.
exec 8>"$toolchain_lock"
flock -s 8

tar -xzf "$runner_archive" -C "$run_dir"

# setup-* actions check the Actions tool cache before downloading. Populate
# each ephemeral job with read-only links to centrally managed cached tools.
# Versions not present here remain job-local and can still be downloaded.
tool_cache="$run_dir/_work/_tool"
shared_tool_cache="$home/.local/share/github-actions-toolcache"
mkdir -p "$tool_cache"
if [[ -d "$shared_tool_cache" ]]; then
  for tool_dir in "$shared_tool_cache"/*; do
    [[ -d "$tool_dir" ]] || continue
    tool_name="$(basename "$tool_dir")"
    mkdir -p "$tool_cache/$tool_name"
    for version_dir in "$tool_dir"/*; do
      [[ -d "$version_dir" ]] || continue
      ln -sfn "$version_dir" "$tool_cache/$tool_name/$(basename "$version_dir")"
    done
  done
fi
export RUNNER_TOOL_CACHE="$tool_cache"
export AGENT_TOOLSDIRECTORY="$tool_cache"

export PATH="$toolchain:$home/.local/bin:$PATH"

# Keep mutable per-job tool state off the shared home configuration so up to
# four concurrent runners cannot race on ~/.gitconfig or Mamba caches.
: >"$job_git_config"
chmod 600 "$job_git_config"
export GIT_CONFIG_GLOBAL="$job_git_config"
export MAMBA_ROOT_PREFIX="$run_dir/mamba-root"
export CONDA_PKGS_DIRS="$run_dir/conda-pkgs"
if command -v git-lfs >/dev/null 2>&1; then
  git lfs install --skip-repo >/dev/null
fi

. "$home/.bash_proxy"
proxy set >/dev/null
git_proxy="$https_proxy"

local_proxy_host='127.0.0.1'
local_proxy_port='18425'
gost_upstream="${git_proxy/socks5h:/socks5:}"
"$home/.local/bin/gost" \
  -L "http://${local_proxy_host}:${local_proxy_port}" \
  -F "$gost_upstream" >"$log_dir/gost.log" 2>&1 &
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
