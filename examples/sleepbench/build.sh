#!/usr/bin/env bash
# Build the sleepbench ENV image on the launcher. The dispatcher
# pins it to its immutable id at submit and ships it to any
# dispatch host that lacks it, so building here is enough.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tag="${1:-sleepbench-env:1}"
ctx="$(mktemp -d)"
trap 'rm -rf "$ctx"' EXIT

cp "$repo_root/examples/sleepbench/Dockerfile" "$ctx/"
mkdir -p "$ctx/dispatcher_sdk"
cp "$repo_root"/src/dispatcher_sdk/*.py "$ctx/dispatcher_sdk/"

docker build -q -t "$tag" "$ctx"
echo "built $tag"
docker image inspect "$tag" --format 'id: {{.Id}}  size: {{.Size}}'
