#!/usr/bin/env bash
set -euo pipefail

go_version="${1:-}"
if [[ -z "$go_version" ]]; then
  echo "usage: $0 <go-version>" >&2
  exit 2
fi

home="$(getent passwd "$(id -u)" | cut -d: -f6)"
test -n "$home"

cache_root="$home/.local/share/github-actions-toolcache"
runner_root="$home/.local/share/github-actions-runner/current"
gh_bin="$home/.local/bin/gh"
gh_run="$home/.local/bin/github-egress-run"
python_bin="$home/.local/opt/github-runner-toolchain/bin/python"

mkdir -p "$cache_root/node" "$cache_root/go"

node_src="$runner_root/externals/node24"
if [[ ! -x "$node_src/bin/node" ]]; then
  echo "runner Node.js 24 runtime not found: $node_src" >&2
  exit 1
fi
node_version="$($node_src/bin/node --version | sed 's/^v//')"
node_version_dir="$cache_root/node/$node_version"
mkdir -p "$node_version_dir"
ln -sfn "$node_src" "$node_version_dir/x64"
touch "$node_version_dir/x64.complete"

release_json="$($gh_run "$gh_bin" api 'repos/actions/go-versions/releases?per_page=100')"
release_tag="$(printf '%s' "$release_json" | "$python_bin" -c '
import json, sys
version = sys.argv[1]
for release in json.load(sys.stdin):
    tag = release.get("tag_name", "")
    if tag.startswith(version + "-"):
        print(tag)
        break
' "$go_version")"
if [[ -z "$release_tag" ]]; then
  echo "no actions/go-versions release found for Go $go_version" >&2
  exit 1
fi

asset_name="go-${go_version}-linux-x64.tar.gz"
asset_json="$($gh_run "$gh_bin" api "repos/actions/go-versions/releases/tags/$release_tag")"
asset_info="$(printf '%s' "$asset_json" | "$python_bin" -c '
import json, sys
name = sys.argv[1]
for asset in json.load(sys.stdin).get("assets", []):
    if asset.get("name") == name:
        print(str(asset["id"]) + "\t" + asset.get("digest", ""))
        break
' "$asset_name")"
if [[ -z "$asset_info" ]]; then
  echo "asset not found: $asset_name in $release_tag" >&2
  exit 1
fi

asset_id="${asset_info%%$'\t'*}"
digest="${asset_info#*$'\t'}"
tmp="$(mktemp -d "$cache_root/.go-${go_version}.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT
archive="$tmp/$asset_name"

$gh_run "$gh_bin" api \
  -H 'Accept: application/octet-stream' \
  "repos/actions/go-versions/releases/assets/$asset_id" >"$archive"

if [[ "$digest" == sha256:* ]]; then
  expected="${digest#sha256:}"
  actual="$(sha256sum "$archive" | awk '{print $1}')"
  if [[ "$actual" != "$expected" ]]; then
    echo "Go toolcache SHA256 mismatch" >&2
    exit 1
  fi
fi

go_version_dir="$cache_root/go/$go_version"
staging="$tmp/extracted"
mkdir -p "$staging"
tar -xzf "$archive" -C "$staging" --strip-components=1

rm -rf "$go_version_dir.new"
mkdir -p "$go_version_dir.new"
mv "$staging" "$go_version_dir.new/x64"
touch "$go_version_dir.new/x64.complete"
rm -rf "$go_version_dir"
mv "$go_version_dir.new" "$go_version_dir"

echo "seeded Node.js $node_version"
echo "seeded Go $go_version from $release_tag"
