#!/bin/sh
# Tests for release-commit.sh and release-check.sh in a scratch repository shaped like this one: a
# root module, all five add-on modules requiring it, go.work replacing that version (needs go and git, no
# network): sh scripts/release.test.sh
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
modules="observability kafka sql mongo rabbitmq"
export GOPROXY=off GOFLAGS=-modcacherw GOMODCACHE="$work/cache" GIT_CONFIG_NOSYSTEM=1

repo=$work/repo
mkdir -p "$repo/scripts"
for m in $modules; do
  mkdir -p "$repo/$m"
done
for s in bump-version.sh local-modules.sh release-commit.sh release-check.sh; do
  cp "$root/scripts/$s" "$repo/scripts/"
done
cd "$repo" || exit 1
printf 'module %s\n\ngo 1.21\n' "$module" >go.mod
printf 'package snodra\n\n// Version is the version.\nconst Version = "0.1.0"\n' >version.go
cat >version_test.go <<'EOF'
package snodra

import (
	"os"
	"testing"
)

func TestVersion(t *testing.T) {
	if want := os.Getenv("SNODRA_RELEASE_VERSION"); want != "" && Version != want {
		t.Errorf("Version is %s, the release is %s", Version, want)
	}
}
EOF
for m in $modules; do
  printf 'module %s/%s\n\ngo 1.21\n\nrequire %s v0.0.0\n' "$module" "$m" "$module" >"$m/go.mod"
  printf 'package %s\n\nimport snodra "%s"\n\n// V is the root version.\nvar V = snodra.Version\n' "$m" "$module" >"$m/o.go"
  printf 'package %s\n\nimport "testing"\n\nfunc TestV(t *testing.T) {\n\tif V == "" {\n\t\tt.Fatal("empty")\n\t}\n}\n' "$m" >"$m/o_test.go"
done
printf 'go 1.21\n\nuse (\n\t.\n' >go.work
for m in $modules; do
  printf '\t./%s\n' "$m" >>go.work
done
printf ')\n\nreplace %s v0.0.0 => ./\n' "$module" >>go.work
git init -q
git config user.name test
git config user.email test@example.invalid
git add -A
git commit -qm "feat: the start"
base=$(git rev-parse HEAD)

# retag VERSION: amend the current changes into HEAD and move all tags there, as a broken
# release step would leave them.
retag() {
  git commit -qa --amend --no-edit
  git tag -f -a "v$1" -m t >/dev/null
  for m in $modules; do
    git tag -f -a "$m/v$1" -m t >/dev/null
  done
}
# check NAME EXPECTED-TEXT VERSION: release-check.sh fails, saying EXPECTED-TEXT.
refused() {
  if out=$(sh scripts/release-check.sh "$3" 2>&1); then
    fail "$1: the check passed"
  elif printf '%s' "$out" | grep -Fq "$2"; then
    echo "ok   $1"
  else
    fail "$1: $(printf '%s' "$out" | tail -3)"
  fi
}

# The workspace builds before any release (go.work replaces v0.0.0).
go build ./...
for m in $modules; do
  (cd "$m" && go test -count=1 ./... >/dev/null)
done && echo "ok   the workspace builds before the first release" || fail "the workspace before the first release"

if sh scripts/release-commit.sh 0.1.0 >/dev/null; then echo "ok   the first release commit"; else fail "the first release commit"; fi
head=$(git rev-parse HEAD)
[ "$(git log -1 --format=%s)" = "release: 0.1.0" ] && [ "$(git rev-parse HEAD^)" = "$base" ] \
  && echo "ok   one commit, release: 0.1.0, on the base" || fail "the commit: $(git log --oneline -3)"

all_tagged=yes
[ "$(git rev-parse 'v0.1.0^{commit}')" = "$head" ] && [ "$(git cat-file -t v0.1.0)" = tag ] || all_tagged=no
for m in $modules; do
  [ "$(git rev-parse "$m/v0.1.0^{commit}")" = "$head" ] && [ "$(git cat-file -t "$m/v0.1.0")" = tag ] || all_tagged=no
done
[ "$all_tagged" = yes ] && echo "ok   six annotated tags on it" || fail "the tags"

expected_files="go.work"
for m in $modules; do
  expected_files="$expected_files $m/go.mod $m/go.sum"
done
actual_files=$(git diff --name-only "$base" HEAD | sort | tr '\n' ' ' | sed 's/ $//')
expected_sorted=$(printf '%s\n' $expected_files | sort | tr '\n' ' ' | sed 's/ $//')
[ "$actual_files" = "$expected_sorted" ] \
  && echo "ok   it writes go.work, all module go.mod and go.sum files" || fail "files: $(git diff --name-only "$base" HEAD)"

all_sums=yes
for m in $modules; do
  [ "$(grep -c "^$module v0.1.0" "$m/go.sum")" -eq 2 ] || all_sums=no
done
[ "$all_sums" = yes ] && echo "ok   all go.sum files have the two root hashes" || fail "go.sum missing root hashes"

out=$(sh scripts/release-check.sh 0.1.0 2>&1) && echo "ok   the check passes" || fail "the check: $out"
sh scripts/release-commit.sh 0.1.0 >/dev/null 2>&1 && fail "a second 0.1.0 is refused" || echo "ok   an existing tag is refused"

sed -e "s|^\($module v0.1.0 h1:\)0|\11|" -e "s|^\($module v0.1.0 h1:\)[^1]|\10|" observability/go.sum >g && mv g observability/go.sum
retag 0.1.0
refused "a tampered h1 line" "checksum mismatch" 0.1.0
git checkout -q "$head" -- observability/go.sum
sed -e "s|^\($module v0.1.0/go.mod h1:\)0|\11|" -e "s|^\($module v0.1.0/go.mod h1:\)[^1]|\10|" observability/go.sum >g && mv g observability/go.sum
retag 0.1.0
refused "a tampered go.mod h1 line" "checksum mismatch" 0.1.0
git checkout -q "$head" -- observability/go.sum
printf 'replace %s => ../\n' "$module" >>observability/go.mod
retag 0.1.0
refused "a replace left in observability/go.mod" "has a replace" 0.1.0
git checkout -q "$head" -- observability/go.mod
printf '*.go export-subst\n' >.gitattributes
git add .gitattributes
retag 0.1.0
refused "an export-subst attribute" "export-subst attribute" 0.1.0
git rm -q .gitattributes
retag 0.1.0
[ "$(git rev-parse 'HEAD^{tree}')" = "$(git rev-parse "$head^{tree}")" ] || fail "the scratch tree is restored"
git tag -f -a observability/v0.1.0 -m t "$base" >/dev/null
refused "a tag on another commit" "does not name HEAD" 0.1.0
git tag -f -a observability/v0.1.0 -m t HEAD >/dev/null
echo x >>version.go
refused "a tree that differs from HEAD" "differs from HEAD" 0.1.0
git checkout -q -- version.go
sh scripts/release-check.sh 0.1.0 >/dev/null 2>&1 && echo "ok   restored, the check passes again" || fail "the restored release"
first=$(git rev-parse HEAD)

# A second release, after a change.
echo '// A change.' >>version.go
git commit -qam "feat: a change"
if sh scripts/release-commit.sh 0.2.0 >/dev/null; then
  ok_second=yes
  grep -qx 'const Version = "0.2.0"' version.go && grep -q "^replace $module v0.2.0 => ./$" go.work || ok_second=no
  for m in $modules; do
    grep -q "^require $module v0.2.0$" "$m/go.mod" || ok_second=no
    [ "$(grep -c "^$module v0.2.0" "$m/go.sum")" -eq 2 ] || ok_second=no
    grep -q "^$module v0.1.0" "$m/go.sum" && ok_second=no
  done
  [ "$ok_second" = yes ] && echo "ok   the second release moves everything to 0.2.0" || fail "second release: $(git show --stat HEAD)"
  out=$(sh scripts/release-check.sh 0.2.0 2>&1) && echo "ok   its check passes" || fail "second check: $out"
else
  fail "the second release commit"
fi

# A bump that forgets version.go: the compiled Version is not the release's.
git reset -q --hard "$first"
git tag -d v0.2.0 observability/v0.2.0 kafka/v0.2.0 sql/v0.2.0 mongo/v0.2.0 rabbitmq/v0.2.0 >/dev/null
echo '// A change.' >>version.go
git commit -qam "feat: a change"
sed '/^rewrite version.go/,/^  { print }.$/d' scripts/bump-version.sh >b && mv b scripts/bump-version.sh && chmod +x scripts/bump-version.sh
git commit -qam "test: a bump without version.go"
sh scripts/release-commit.sh 0.2.0 >/dev/null || fail "the release commit without version.go"
refused "a forgotten Version" "Version is 0.1.0, the release is 0.2.0" 0.2.0

git reset -q --hard "$first"
printf 'x\n' >>observability/go.mod
sh scripts/release-commit.sh 0.3.0 >/dev/null 2>&1 && fail "a dirty tree is refused" || echo "ok   a dirty tree is refused"

exit $failed
