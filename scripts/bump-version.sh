#!/bin/sh
# Writes a version into every file of this repository that carries it. Run from the repository root:
#
#   scripts/bump-version.sh 0.2.0
#
# - version.go: the Version constant (the client version the library announces to the hub).
# - <module>/go.mod (observability, kafka, sql, mongo, rabbitmq): requirement on the root module becomes vX.Y.Z,
#   the version tagged with it. A consumer resolves each module through that line, so it must name a release.
# - go.work: the workspace replaces that same version with the root at HEAD, so development and CI
#   build the modules against the code next to them, never against a download.
#
# <module>/go.sum is not written here: the release job lets `go mod tidy` record the root
# module's hashes once the tag exists (see the version job in .github/workflows/ci.yml).
#
# Fails, changing nothing further, when a file no longer has the line it expects, so a moved
# version is caught by the tests (scripts/bump-version.test.sh) and not by a release.
set -eu

module=github.com/snodra/snodra-messaging-go

version=${1:-}
grep -Eqx '(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)' <<EOF || { echo "usage: $0 X.Y.Z" >&2; exit 2; }
$version
EOF

# rewrite FILE AWK-PROGRAM: the program sets done=1 when it replaced the version.
rewrite() {
  if awk -v v="$version" -v module="$module" "$2"' END { exit !done }' "$1" >"$1.tmp"; then
    mv "$1.tmp" "$1"
  else
    rm -f "$1.tmp"
    echo "bump-version: no version to replace in $1" >&2
    exit 1
  fi
}

rewrite version.go '
  /^const Version = "/ { $0 = "const Version = \"" v "\""; done = 1 }
  { print }'

# The requirement on exactly this module path, in a require block or on a require line.
for m in observability kafka sql mongo rabbitmq; do
  rewrite "$m/go.mod" '
    /^\)/ { block = 0 }
    (block && $1 == module) || ($1 == "require" && $2 == module) {
      sub(/ v[^ ]+/, " v" v)
      done = 1
    }
    /^require \($/ { block = 1 }
    { print }'
done

rewrite go.work '
  $1 == "replace" && $2 == module && $4 == "=>" { $3 = "v" v; done = 1 }
  { print }'
