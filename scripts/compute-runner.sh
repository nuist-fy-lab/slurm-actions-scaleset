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
runtime_config="${RUNNER_RUNTIME_CONFIG:-$home/.config/slurm-actions-scaleset/runtime.conf}"
test -r "$runtime_config"
. "$runtime_config"

: "${RUNNER_WORK_ROOT:?RUNNER_WORK_ROOT is required}"
: "${TOOLCHAIN_BIN:?TOOLCHAIN_BIN is required}"
: "${SHARED_TOOL_CACHE:?SHARED_TOOL_CACHE is required}"
: "${TOOLCHAIN_LOCK_FILE:?TOOLCHAIN_LOCK_FILE is required}"
: "${GOST_BIN:?GOST_BIN is required}"
: "${LOCAL_HTTP_PROXY_HOST:?LOCAL_HTTP_PROXY_HOST is required}"
: "${LOCAL_HTTP_PROXY_PORT:?LOCAL_HTTP_PROXY_PORT is required}"
: "${GIT_TARGET_URL_PREFIX:?GIT_TARGET_URL_PREFIX is required}"
: "${GIT_EGRESS_SCHEME:?GIT_EGRESS_SCHEME is required}"
: "${GIT_EGRESS_HOST:?GIT_EGRESS_HOST is required}"
: "${GIT_EGRESS_USERNAME:?GIT_EGRESS_USERNAME is required}"
: "${EGRESS_TOKEN_FILE:?EGRESS_TOKEN_FILE is required}"
: "${EGRESS_ENV_FILE:?EGRESS_ENV_FILE is required}"

test -x "$GOST_BIN"
test -r "$EGRESS_TOKEN_FILE"
test -r "$EGRESS_ENV_FILE"

toolchain="$TOOLCHAIN_BIN"
run_dir="${RUNNER_WORK_ROOT%/}/github-scale-set-${SLURM_JOB_ID}-${runner_name}"
log_dir="$state_dir/runners/${runner_name}-${SLURM_JOB_ID}"
toolchain_lock="$TOOLCHAIN_LOCK_FILE"
job_git_config="$run_dir/gitconfig"
gost_config="$run_dir/gost.json"
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
shared_tool_cache="$SHARED_TOOL_CACHE"
mkdir -p "$tool_cache"
if [[ -d "$shared_tool_cache" ]]; then
  for tool_dir in "$shared_tool_cache"/*; do
    [[ -d "$tool_dir" ]] || continue
    tool_name="$(basename "$tool_dir")"
    mkdir -p "$tool_cache/$tool_name"
    for version_dir in "$tool_dir"/*; do
      [[ -d "$version_dir" ]] || continue
      # Only expose administrator-completed immutable cache entries. An
      # incomplete shared version must never become a writable target for
      # setup-* actions from multiple concurrent runners.
      if ! find "$version_dir" -maxdepth 1 -type f -name '*.complete' -print -quit | grep -q .; then
        continue
      fi
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

# Build the dedicated GitHub Git proxy entry from the protected egress token
# without placing credentials in argv or exported environment variables.
JOB_GIT_CONFIG="$job_git_config" \
GIT_TARGET_URL_PREFIX="$GIT_TARGET_URL_PREFIX" \
GIT_EGRESS_SCHEME="$GIT_EGRESS_SCHEME" \
GIT_EGRESS_HOST="$GIT_EGRESS_HOST" \
GIT_EGRESS_USERNAME="$GIT_EGRESS_USERNAME" \
EGRESS_TOKEN_FILE="$EGRESS_TOKEN_FILE" \
"$toolchain/python" <<'PY'
import os
from pathlib import Path
from urllib.parse import quote

path = Path(os.environ["JOB_GIT_CONFIG"])
token = Path(os.environ["EGRESS_TOKEN_FILE"]).read_text().strip()
if not token:
    raise SystemExit("egress token is empty")

scheme = os.environ["GIT_EGRESS_SCHEME"]
host = os.environ["GIT_EGRESS_HOST"]
username = quote(os.environ["GIT_EGRESS_USERNAME"], safe="")
password = quote(token, safe="")
target = os.environ["GIT_TARGET_URL_PREFIX"]
proxy = f"{scheme}://{username}:{password}@{host}"
path.write_text(f'[http "{target}"]\n\tproxy = {proxy}\n')
PY
chmod 600 "$job_git_config"

if command -v git-lfs >/dev/null 2>&1; then
  git lfs install --skip-repo >/dev/null
fi

. "$home/.bash_proxy"
proxy set >/dev/null

# Convert the authenticated SOCKS endpoint produced by `proxy set` into a
# loopback HTTP CONNECT proxy for Node-based Actions. The upstream credential
# is written only to an owner-readable per-job config, never to process argv.
UPSTREAM_PROXY_URL="$https_proxy" \
GOST_CONFIG="$gost_config" \
LOCAL_HTTP_PROXY_HOST="$LOCAL_HTTP_PROXY_HOST" \
LOCAL_HTTP_PROXY_PORT="$LOCAL_HTTP_PROXY_PORT" \
GOST_LOG_LEVEL="${GOST_LOG_LEVEL:-warn}" \
"$toolchain/python" <<'PY'
import json
import os
from pathlib import Path
from urllib.parse import unquote, urlsplit

url = urlsplit(os.environ["UPSTREAM_PROXY_URL"])
if url.scheme not in {"socks5", "socks5h"} or not url.hostname or not url.port:
    raise SystemExit("proxy set did not produce a usable SOCKS5 endpoint")

connector = {"type": "socks5"}
if url.username is not None or url.password is not None:
    connector["auth"] = {
        "username": unquote(url.username or ""),
        "password": unquote(url.password or ""),
    }

config = {
    "services": [{
        "name": "runner-http-proxy",
        "addr": f'{os.environ["LOCAL_HTTP_PROXY_HOST"]}:{os.environ["LOCAL_HTTP_PROXY_PORT"]}',
        "handler": {"type": "http", "chain": "runner-chain"},
        "listener": {"type": "tcp"},
    }],
    "chains": [{
        "name": "runner-chain",
        "hops": [{
            "name": "runner-hop",
            "nodes": [{
                "name": "runner-upstream",
                "addr": f"{url.hostname}:{url.port}",
                "connector": connector,
                "dialer": {"type": "tcp"},
            }],
        }],
    }],
    "log": {
        "output": "stderr",
        "level": os.environ["GOST_LOG_LEVEL"],
        "format": "json",
    },
}

path = Path(os.environ["GOST_CONFIG"])
path.write_text(json.dumps(config))
path.chmod(0o600)
PY

# Do not leak the authenticated upstream proxy into the GOST environment.
unset http_proxy https_proxy all_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY ftp_proxy FTP_PROXY

"$GOST_BIN" -C "$gost_config" >"$log_dir/gost.log" 2>&1 &
bridge_pid=$!

"$toolchain/python" - "$LOCAL_HTTP_PROXY_HOST" "$LOCAL_HTTP_PROXY_PORT" <<'PY'
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
rm -f "$gost_config"

runner_proxy="http://${LOCAL_HTTP_PROXY_HOST}:${LOCAL_HTTP_PROXY_PORT}"
export HTTP_PROXY="$runner_proxy" HTTPS_PROXY="$runner_proxy"
export http_proxy="$runner_proxy" https_proxy="$runner_proxy"
unset ALL_PROXY all_proxy FTP_PROXY ftp_proxy
. "$EGRESS_ENV_FILE"

export ACTIONS_RUNNER_INPUT_JITCONFIG="$(cat "$jit_file")"
rm -f "$jit_file"

cd "$run_dir"
./run.sh
