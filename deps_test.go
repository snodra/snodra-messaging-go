package snodramessaging

import (
	"os"
	"strings"
	"testing"
)

func TestCoreRequiresOnlyGRPCAndProtobuf(t *testing.T) {
	data, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"google.golang.org/grpc": true, "google.golang.org/protobuf": true}
	inBlock := false
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		var req string
		switch {
		case line == "require (":
			inBlock = true
			continue
		case inBlock && line == ")":
			inBlock = false
			continue
		case inBlock:
			req = line
		case strings.HasPrefix(line, "require "):
			req = strings.TrimPrefix(line, "require ")
		default:
			continue
		}
		if strings.Contains(req, "// indirect") {
			continue
		}
		if f := strings.Fields(req); len(f) > 0 && !allowed[f[0]] {
			t.Errorf("direct requirement %s is not allowed", f[0])
		}
	}
}
