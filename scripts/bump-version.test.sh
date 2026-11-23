#!/bin/sh
# Tests for bump-version.sh against copies of this repository's files: sh scripts/bump-version.test.sh
set -u
root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
failed=0

fail() {
  echo "FAIL $*"
  failed=1
}

module=github.com/snodra/snodra-messaging-go
files="version.go observability/go.mod go.work"

mkdir "$work/scripts" "$work/observability"
for f in $files; do cp "$root/$f" "$work/$f"; done
cp "$root/scripts/bump-version.sh" "$work/scripts/"
cd "$work" || exit 1

if sh scripts/bump-version.sh 9.8.7; then echo "ok   it runs"; else fail "it runs"; fi

grep -qx 'const Version = "9.8.7"' version.go && echo "ok   version.go" || fail "version.go"

req=$(awk -v m="$module" '$1 == m { print $2 }' observability/go.mod)
[ "$req" = v9.8.7 ] && echo "ok   observability/go.mod requires the root at v9.8.7" || fail "observability/go.mod: '$req'"

grep -qx "replace $module v9.8.7 => ./" go.work && echo "ok   go.work replaces v9.8.7" || fail "go.work"

# Only those lines change: one in each file.
for f in $files; do
  n=$(diff "$root/$f" "$f" | grep -c '^>')
  [ "$n" -eq 1 ] && echo "ok   one line in $f" || fail "$n lines changed in $f"
done

# Twice is the same as once: the release of the version already written still runs.
for f in $files; do cp "$f" "$f.once"; done
sh scripts/bump-version.sh 9.8.7 || fail "a second run"
for f in $files; do cmp -s "$f" "$f.once" || fail "a second run changed $f"; done
echo "ok   a second run changes nothing"

# The single-line form, and only the exact module path: not observability itself, not a longer name.
cat >observability/go.mod <<EOF
module $module/observability

go 1.26.0

require $module v0.1.0

require (
	${module}2 v0.3.0
	$module/observability v0.3.0 // indirect
)
EOF
if sh scripts/bump-version.sh 9.8.8; then
  got=$(grep -c 'v9\.8\.8' observability/go.mod)
  grep -qx "require $module v9.8.8" observability/go.mod && [ "$got" -eq 1 ] \
    && echo "ok   a require line, and only the exact path" || fail "single-line require: $(cat observability/go.mod)"
else
  fail "a single-line require"
fi

# A file without the expected line fails the bump.
printf 'package snodra\n' >version.go
if sh scripts/bump-version.sh 9.8.9 2>/dev/null; then fail "a missing Version fails"; else echo "ok   a missing Version fails"; fi
cp version.go.once version.go

printf 'module %s/observability\n\ngo 1.26.0\n' "$module" >observability/go.mod
if sh scripts/bump-version.sh 9.8.9 2>/dev/null; then fail "a missing requirement fails"; else echo "ok   a missing requirement fails"; fi
cp observability/go.mod.once observability/go.mod

printf 'go 1.26.0\n\nuse (\n\t.\n\t./observability\n)\n' >go.work
if sh scripts/bump-version.sh 9.8.9 2>/dev/null; then fail "a missing go.work replace fails"; else echo "ok   a missing go.work replace fails"; fi

refused=yes
for v in 1.x 1.2 01.2.3 1.2.3-rc1 v1.2.3 ''; do
  if sh scripts/bump-version.sh "$v" 2>/dev/null; then fail "the version '$v' is refused"; refused=no; fi
done
[ $refused = yes ] && echo "ok   a malformed version fails"

exit $failed
