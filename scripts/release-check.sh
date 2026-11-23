#!/bin/sh
# The packaging check of a release commit, before anything is pushed: the Go proxy keeps a
# published tag forever. Run from the repository root with the release commit checked out:
#
#   scripts/release-check.sh 0.2.0
#
# - vX.Y.Z and observability/vX.Y.Z both name HEAD, and the tree is clean.
# - No file has the export-subst attribute: git archive, which both this check and the Go proxy
#   use to build the module zip, would then write ref names into it, and the hashes could differ.
# - The root module as tagged, with no workspace: it builds, vets, is tidy, and its whole test
#   suite passes with SNODRA_RELEASE_VERSION=X.Y.Z (TestVersion compares the compiled Version).
# - observability/go.mod requires the root at vX.Y.Z and has no replace directive.
# - observability/ as a consumer gets it (scripts/local-modules.sh): both modules fetched from the
#   new tags into an empty module cache; it is tidy, builds, vets and its tests pass. A root hash
#   other than observability/go.sum's is a checksum error there.
# - observability/go.sum carries exactly the two hashes computed from the tag.
set -eu

die() {
  echo "release-check: $*" >&2
  exit 1
}

module=github.com/snodra/snodra-messaging-go
version=${1:-}
grep -Eqx '(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)' <<EOF || die "usage: $0 X.Y.Z"
$version
EOF

head=$(git rev-parse HEAD)
for t in "v$version" "observability/v$version"; do
  [ "$(git rev-parse -q --verify "refs/tags/$t^{commit}" || true)" = "$head" ] || die "the tag $t does not name HEAD"
done
[ -z "$(git status --porcelain)" ] || die "the tree differs from HEAD"

if git ls-files | git check-attr --stdin export-subst | grep -v ': export-subst: unspecified$'; then
  die "a file has the export-subst attribute"
fi

GOWORK=off go build ./...
GOWORK=off go vet ./...
GOWORK=off go mod tidy -diff
SNODRA_RELEASE_VERSION="$version" GOWORK=off go test -count=1 ./...

awk -v m="$module" -v t="v$version" '
  $1 == "replace" { replaced = 1 }
  /^\)/ { block = 0 }
  (block && $1 == m && $2 == t) || ($1 == "require" && $2 == m && $3 == t) { required = 1 }
  /^require \($/ { block = 1 }
  END { exit !(required && !replaced) }' observability/go.mod \
  || die "observability/go.mod does not require the root module at v$version, or has a replace"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
# shellcheck disable=SC2016 # expanded by the inner shell
scripts/local-modules.sh sh -c '
  set -e
  root=$PWD
  cd "$1"
  go mod download -json "$2@v$3" > root-module.json
  go mod download -json "$2/observability@v$3" > /dev/null
  cd "$root/observability"
  go mod tidy -diff
  go build ./...
  go vet ./...
  go test -count=1 ./...
' sh "$work" "$module" "$version"

field() { sed -n "s/^[[:space:]]*\"$1\": \"\(.*\)\",\{0,1\}$/\1/p" "$work/root-module.json"; }
sum=$(field Sum)
modsum=$(field GoModSum)
grep -Fqx "$module v$version $sum" observability/go.sum || die "observability/go.sum lacks $module v$version $sum"
grep -Fqx "$module v$version/go.mod $modsum" observability/go.sum || die "observability/go.sum lacks $module v$version/go.mod $modsum"
echo "release-check: v$version and observability/v$version at $head package"
echo "$module v$version $sum"
echo "$module v$version/go.mod $modsum"
