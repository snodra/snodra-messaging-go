#!/usr/bin/env bash
# Regenerates internal/hubpb from proto/hub.proto with pinned tools. hub.proto has no go_package (it must stay
# byte-identical to the server's), so the Go package is mapped with M options.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

want="libprotoc 33.4"
have="$(protoc --version)"
if [ "$have" != "$want" ]; then
  echo "gen-proto: need '$want', found '$have'" >&2
  exit 1
fi

# One install per module: `go install` refuses packages of different modules in one call.
GOBIN="$root/.bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
GOBIN="$root/.bin" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2

pkg=github.com/snodra/snodra-messaging-go/internal/hubpb
protoc -I proto \
  --plugin=protoc-gen-go="$root/.bin/protoc-gen-go" \
  --plugin=protoc-gen-go-grpc="$root/.bin/protoc-gen-go-grpc" \
  --go_out=internal/hubpb --go_opt=paths=source_relative --go_opt=Mhub.proto="$pkg" \
  --go-grpc_out=internal/hubpb --go-grpc_opt=paths=source_relative --go-grpc_opt=Mhub.proto="$pkg" \
  hub.proto
