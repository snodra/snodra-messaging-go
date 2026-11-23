#!/bin/sh
# Tests for local-modules.sh in a scratch repository shaped like this one (needs go and git, no
# network): sh scripts/local-modules.test.sh
set -u
root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'chmod -R u+w "$work" 2>/dev/null; rm -rf "$work"' EXIT
failed=0

fail() {
  echo "FAIL $*"
  failed=1
}

module=github.com/snodra/snodra-messaging-go

mkdir -p "$work/repo/scripts" "$work/repo/observability" "$work/out"
cp "$root/scripts/local-modules.sh" "$work/repo/scripts/"
cd "$work/repo" || exit 1
git init -q
git config user.name test
git config user.email test@example.invalid
printf 'module %s\n\ngo 1.21\n' "$module" >go.mod
printf 'package snodra\n\nconst Version = "0.0.1"\n' >version.go
printf 'module %s/observability\n\ngo 1.21\n' "$module" >observability/go.mod
printf 'package observability\n' >observability/doc.go
git add -A
git commit -qm init
git tag v0.0.1
git tag observability/v0.0.1
head=$(git rev-parse HEAD)

# Run from outside any module; GOPROXY=off proves nothing comes from the network.
cd "$work/out" || exit 1
local_modules() { GOPROXY=off sh "$work/repo/scripts/local-modules.sh" "$@"; }

# shellcheck disable=SC2016 # expanded by the inner shell
if local_modules sh -c '
    go mod download -json "$0@v0.0.1" >root.json
    go mod download -json "$0/observability@v0.0.1" >observability.json
    dir=$(sed -n "s/^.*\"Dir\": \"\(.*\)\",$/\1/p" root.json)
    ls -A "$dir" | tr "\n" " " >root-files.txt
    go env GOWORK GOMODCACHE >env.txt' "$module"; then
  grep -q "\"Hash\": \"$head\"" root.json && grep -q '"Version": "v0.0.1"' root.json \
    && echo "ok   a tag no remote has resolves, from this repository" || fail "root: $(cat root.json)"
  grep -q "\"Hash\": \"$head\"" observability.json && grep -q '"Ref": "refs/tags/observability/v0.0.1"' observability.json \
    && echo "ok   observability resolves at its own tag" || fail "observability: $(cat observability.json)"
  [ "$(cat root-files.txt)" = "go.mod scripts version.go " ] \
    && echo "ok   the root module holds none of observability/" || fail "root module files: $(cat root-files.txt)"
  cache=$(sed -n 2p env.txt)
  [ "$(sed -n 1p env.txt)" = off ] && echo "ok   the workspace is off" || fail "GOWORK: $(sed -n 1p env.txt)"
  case $cache in
    "$(go env GOMODCACHE)" | '') fail "the module cache is the usual one: '$cache'" ;;
    *) [ ! -e "$cache" ] && echo "ok   a new module cache, removed afterwards" || fail "the cache $cache is left" ;;
  esac
else
  fail "the downloads"
fi

if local_modules go mod download "$module@v0.0.2" 2>/dev/null; then
  fail "a tag this repository lacks fails"
else
  echo "ok   a tag this repository lacks fails"
fi

local_modules sh -c 'exit 3'
status=$?
[ $status -eq 3 ] && echo "ok   the command's exit status" || fail "exit status $status"

exit $failed
