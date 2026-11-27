#!/bin/sh
# Makes the release commit of a version on HEAD and tags it locally. Run from the
# repository root, on a clean tree, with a git identity set:
#
#   scripts/release-commit.sh 0.2.0
#
# The commit is `release: X.Y.Z`; the annotated tags are vX.Y.Z (the root module) and
# <module>/vX.Y.Z for each of observability, kafka, sql, mongo, rabbitmq, all on that commit.
# Nothing is pushed.
#
# Each <module>/go.sum must hold the root module's hashes at vX.Y.Z, which exist only once that
# tag does. So the bumped commit is tagged for a moment, `go mod tidy` fetches the root module from
# it as a consumer's go command fetches it from GitHub (scripts/local-modules.sh) and records the
# hashes, and the commit is amended. The root module holds no file of the nested modules, so the
# amended commit's root module, and its hashes, are the first commit's.
# scripts/release-check.sh then checks the result from the final tags.
set -eu

die() {
  echo "release-commit: $*" >&2
  exit 1
}

modules="observability kafka sql mongo rabbitmq"

version=${1:-}
[ -z "$(git status --porcelain --untracked-files=no)" ] || die "the tree has uncommitted changes"
for t in "v$version" observability/v$version kafka/v$version sql/v$version mongo/v$version rabbitmq/v$version; do
  if git rev-parse -q --verify "refs/tags/$t" >/dev/null; then die "the tag $t exists already"; fi
done

scripts/bump-version.sh "$version"
git commit -qam "release: $version"
git tag "v$version"
for m in $modules; do
  if ! (cd "$m" && ../scripts/local-modules.sh go mod tidy); then
    git tag -d "v$version" >/dev/null
    die "go mod tidy in $m/ failed"
  fi
done
git tag -d "v$version" >/dev/null

# go.sum by name: tidy may create it.
for m in $modules; do
  git add "$m/go.mod" "$m/go.sum"
done
git commit -qa --amend --no-edit
git tag -a "v$version" -m "snodra-messaging-go v$version"
for m in $modules; do
  git tag -a "$m/v$version" -m "snodra-messaging-go/$m v$version"
done
git show --stat --format='%H %s' HEAD
