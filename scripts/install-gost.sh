#!/usr/bin/env bash
set -euo pipefail

version="${GOST_VERSION:-3.3.0}"
expected_sha256="${GOST_SHA256:-676fb7f78d267b6ae73df719c0c7f2b565dde7147da935cfafbc1e1da558b6d5}"
gh_fetch="${GH_FETCH:-$HOME/.local/bin/gh-fetch}"
target="${GOST_TARGET:-$HOME/.local/bin/gost}"

archive="gost_${version}_linux_amd64.tar.gz"
url="https://github.com/go-gost/gost/releases/download/v${version}/${archive}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

"$gh_fetch" "$url" -o "$tmp/$archive"
actual_sha256="$(sha256sum "$tmp/$archive" | awk '{print $1}')"
if [[ "$actual_sha256" != "$expected_sha256" ]]; then
  echo "GOST checksum mismatch" >&2
  exit 1
fi

tar -xzf "$tmp/$archive" -C "$tmp"
binary="$(find "$tmp" -maxdepth 2 -type f -name gost -perm -u+x | head -n 1)"
if [[ -z "$binary" ]]; then
  echo "GOST binary not found in release archive" >&2
  exit 1
fi

mkdir -p "$(dirname "$target")"
install -m 0755 "$binary" "$target"
"$target" -V
