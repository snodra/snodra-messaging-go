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
modules="observability kafka sql mongo rabbitmq"
files="version.go go.work"
for m in $modules; do
  files="$files $m/go.mod"
done

mkdir "$work/scripts"
for m in $modules; do
  mkdir "$work/$m"
done
for f in $files; do cp "$root/$f" "$work/$f"; done
cp "$root/scripts/bump-version.sh" "$work/scripts/"
cd "$work" || exit 1

if sh scripts/bump-version.sh 9.8.7; then echo "ok   it runs"; else fail "it runs"; fi

grep -qx 'const Version = "9.8.7"' version.go && echo "ok   version.go" || fail "version.go"

for m in $modules; do
  req=$(awk -v m="$module" '$1 == m { print $2 }' "$m/go.mod")
  [ "$req" = v9.8.7 ] && echo "ok   $m/go.mod requires the root at v9.8.7" || fail "$m/go.mod: '$req'"
done

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

# The single-line form, and only the exact module path: not the module itself, not a longer name.
for m in $modules; do
  cat >"$m/go.mod" <<EOF
module $module/$m

go 1.26.0

require $module v0.1.0

require (
	${module}2 v0.3.0
	$module/$m v0.3.0 // indirect
)
EOF
done

if sh scripts/bump-version.sh 9.8.8; then
  for m in $modules; do
    got=$(grep -c 'v9\.8\.8' "$m/go.mod")
    grep -qx "require $module v9.8.8" "$m/go.mod" && [ "$got" -eq 1 ] \
      && echo "ok   $m single-line require, and only the exact path" || fail "$m single-line require: $(cat "$m/go.mod")"
  done
else
  fail "a single-line require"
fi
for f in $files; do cp "$f.once" "$f"; done

# A file without the expected line fails the bump.
printf 'package snodra\n' >version.go
if sh scripts/bump-version.sh 9.8.9 2>/dev/null; then fail "a missing Version fails"; else echo "ok   a missing Version fails"; fi
cp version.go.once version.go

for m in $modules; do
  printf 'module %s/%s\n\ngo 1.26.0\n' "$module" "$m" >"$m/go.mod"
  if sh scripts/bump-version.sh 9.8.9 2>/dev/null; then fail "a missing $m requirement fails"; else echo "ok   a missing $m requirement fails"; fi
  cp "$m/go.mod.once" "$m/go.mod"
done

printf 'go 1.26.0\n\nuse (\n\t.\n\t./observability\n)\n' >go.work
if sh scripts/bump-version.sh 9.8.9 2>/dev/null; then fail "a missing go.work replace fails"; else echo "ok   a missing go.work replace fails"; fi
cp go.work.once go.work

refused=yes
for v in 1.x 1.2 01.2.3 1.2.3-rc1 v1.2.3 ''; do
  if sh scripts/bump-version.sh "$v" 2>/dev/null; then fail "the version '$v' is refused"; refused=no; fi
done
[ $refused = yes ] && echo "ok   a malformed version fails"

exit $failed
