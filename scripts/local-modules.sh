#!/bin/sh
# Runs a command the way a consumer's go command sees this repository, but without the network for
# it: the modules github.com/snodra/snodra-messaging-go and .../observability are fetched with git
# from this repository's own tags, which is how proxy.golang.org fetches them from GitHub (the go
# command's direct mode builds the module zips and hashes them the same way). A tag that is not
# pushed yet therefore resolves, with the hashes the checksum database will record for it.
#
#   scripts/local-modules.sh go mod download -json github.com/snodra/snodra-messaging-go@v0.2.0
#   scripts/local-modules.sh sh -c 'cd observability && go build ./...'
#
# - The workspace is off (GOWORK=off): go.work is for development, consumers never read it.
# - The module cache is new and empty, and removed afterwards, so nothing computed before is reused.
# - These two module paths skip the proxy and the checksum database (GOPRIVATE): observability/go.sum
#   is the only check, and a hash that differs from it fails the go command. Every other module
#   comes from GOPROXY and is checked as usual.
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)
module=github.com/snodra/snodra-messaging-go

cache=$(mktemp -d)
trap 'rm -rf "$cache"' EXIT

# git reads https://github.com/snodra/snodra-messaging-go as this repository (an insteadOf rule
# passed in the environment, added to any the caller passes the same way).
n=${GIT_CONFIG_COUNT:-0}
export GIT_CONFIG_COUNT=$((n + 1))
export "GIT_CONFIG_KEY_$n=url.file://$root.insteadOf"
export "GIT_CONFIG_VALUE_$n=https://$module"

export GOWORK=off
export GOMODCACHE="$cache"
export GOFLAGS="${GOFLAGS:+$GOFLAGS }-modcacherw"
export GOPRIVATE="$module" GONOPROXY="$module" GONOSUMDB="$module"

"$@"
