#!/bin/sh
# Makes the release commit of a version on HEAD and tags it twice, locally. Run from the
# repository root, on a clean tree, with a git identity set:
#
#   scripts/release-commit.sh 0.2.0
#
# The commit is `release: X.Y.Z`; the annotated tags are vX.Y.Z (the root module) and
# observability/vX.Y.Z (the module in observability/), both on that commit. Nothing is pushed.
#
# observability/go.sum must hold the root module's hashes at vX.Y.Z, which exist only once that
# tag does. So the bumped commit is tagged for a moment, `go mod tidy` fetches the root module from
# it as a consumer's go command fetches it from GitHub (scripts/local-modules.sh) and records the
# hashes, and the commit is amended. The root module holds no file of observability/ (a nested
# module), so the amended commit's root module, and its hashes, are the first commit's.
# scripts/release-check.sh then checks the result from the final tags.
set -eu

die() {
  echo "release-commit: $*" >&2
  exit 1
}

version=${1:-}
[ -z "$(git status --porcelain --untracked-files=no)" ] || die "the tree has uncommitted changes"
for t in "v$version" "observability/v$version"; do
  if git rev-parse -q --verify "refs/tags/$t" >/dev/null; then die "the tag $t exists already"; fi
done

scripts/bump-version.sh "$version"
git commit -qam "release: $version"
git tag "v$version"
if ! (cd observability && ../scripts/local-modules.sh go mod tidy); then
  git tag -d "v$version" >/dev/null
  die "go mod tidy in observability/ failed"
fi
git tag -d "v$version" >/dev/null
# go.sum by name: tidy may create it.
git add observability/go.mod observability/go.sum
git commit -qa --amend --no-edit
git tag -a "v$version" -m "snodra-messaging-go v$version"
git tag -a "observability/v$version" -m "snodra-messaging-go/observability v$version"
git show --stat --format='%H %s' HEAD
