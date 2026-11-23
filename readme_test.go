package snodra

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// goBlocks are the contents of the ```go fences of text, in order.
func goBlocks(text string) []string {
	var blocks []string
	for _, m := range regexp.MustCompile("(?s)```go\n(.*?)```").FindAllStringSubmatch(text, -1) {
		blocks = append(blocks, m[1])
	}
	return blocks
}

func readText(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestReadmeLinksArePublic(t *testing.T) {
	// Links in the prose; the code blocks name example hosts.
	readme := regexp.MustCompile("(?s)```.*?```").ReplaceAllString(readText(t, "README.md"), "")
	links := regexp.MustCompile(`https?://[^\s)>"'`+"`"+`]+`).FindAllString(readme, -1)
	if len(links) == 0 {
		t.Fatal("the README has no links")
	}
	for _, l := range links {
		l = strings.TrimRight(l, ".,;")
		switch {
		case strings.HasPrefix(l, "https://snodra.damoreira.com/"),
			strings.HasPrefix(l, "https://github.com/snodra/"),
			strings.HasPrefix(l, "https://pkg.go.dev/"):
		default:
			t.Errorf("the README links to %s", l)
		}
	}
}

// The install lines name no version, so `go get` takes the latest release and a release never
// has to edit the README (scripts/bump-version.sh does not touch it). Both modules are named.
func TestReadmeInstallLinesNameNoVersion(t *testing.T) {
	modules := map[string]bool{}
	for _, line := range strings.Split(readText(t, "README.md"), "\n") {
		if !strings.HasPrefix(line, "go get ") {
			continue
		}
		f := strings.Fields(line)
		if len(f) < 3 || strings.Contains(f[2], "@") {
			t.Errorf("an install line names a version: %s", line)
			continue
		}
		modules[f[2]] = true
	}
	for _, m := range []string{"github.com/snodra/snodra-messaging-go", "github.com/snodra/snodra-messaging-go/observability"} {
		if !modules[m] {
			t.Errorf("the README has no install line for %s", m)
		}
	}
}

// The first Go block of the README is the quick start: the service code between the markers of
// examples/quickstart/main.go, the markers themselves left out.
func TestReadmeQuickstartIsTheCompiledOne(t *testing.T) {
	const beginMarker, endMarker = "// quickstart:begin", "// quickstart:end"
	src := readText(t, "examples/quickstart/main.go")
	begin := strings.Index(src, beginMarker)
	end := strings.Index(src, endMarker)
	if begin < 0 || end < begin+len(beginMarker) {
		t.Fatal("examples/quickstart/main.go lacks its quickstart markers")
	}
	want := strings.TrimSpace(src[begin+len(beginMarker) : end])
	if !strings.HasPrefix(want, "package main") || strings.Contains(want, "quickstart:") {
		t.Fatalf("the quick start between the markers is not the whole program:\n%s", want)
	}
	blocks := goBlocks(readText(t, "README.md"))
	if len(blocks) == 0 || strings.TrimSpace(blocks[0]) != want {
		t.Fatal("the README's first Go block differs from examples/quickstart/main.go between its markers")
	}
}

// snippets are the regions between "// readme:begin <name>" and "// readme:end" of the example
// files, one tab of indentation removed.
func snippets(t *testing.T, path string) []string {
	t.Helper()
	var out []string
	var cur []string
	in := false
	for _, line := range strings.Split(readText(t, path), "\n") {
		switch trimmed := strings.TrimSpace(line); {
		case strings.HasPrefix(trimmed, "// readme:begin"):
			in, cur = true, nil
		case trimmed == "// readme:end":
			if in {
				out = append(out, strings.Join(cur, "\n"))
			}
			in = false
		case in:
			cur = append(cur, strings.TrimPrefix(line, "\t"))
		}
	}
	return out
}

// Every other Go block of the README is a region of example_test.go (or observability/example_test.go),
// which the compiler checks, and every such region is in the README.
func TestEveryReadmeSnippetIsCompiled(t *testing.T) {
	compiled := append(snippets(t, "example_test.go"), snippets(t, "observability/example_test.go")...)
	blocks := goBlocks(readText(t, "README.md"))
	if len(blocks) < 2 {
		t.Fatal("the README has no snippets after the quick start")
	}
	inReadme := map[string]bool{}
	for _, b := range blocks[1:] {
		inReadme[strings.TrimSpace(b)] = true
	}
	known := map[string]bool{}
	for _, c := range compiled {
		known[strings.TrimSpace(c)] = true
		if !inReadme[strings.TrimSpace(c)] {
			t.Errorf("a compiled example is not in the README:\n%s", c)
		}
	}
	for _, b := range blocks[1:] {
		if !known[strings.TrimSpace(b)] {
			t.Errorf("a README snippet is not compiled by an example test:\n%s", b)
		}
	}
}
