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
			strings.HasPrefix(l, "https://pkg.go.dev/"),
			strings.HasPrefix(l, "https://img.shields.io/"): // the badges
		default:
			t.Errorf("the README links to %s", l)
		}
	}
}

// The install lines ask for @latest and name no version, so a release never has to edit the
// README (scripts/bump-version.sh does not touch it). Both modules are named.
func TestReadmeInstallLinesNameNoVersion(t *testing.T) {
	modules := map[string]bool{}
	for _, line := range strings.Split(readText(t, "README.md"), "\n") {
		if !strings.HasPrefix(line, "go get ") {
			continue
		}
		f := strings.Fields(line)
		module, ok := "", len(f) >= 3
		if ok {
			module, ok = strings.CutSuffix(f[2], "@latest")
		}
		if !ok || strings.Contains(module, "@") {
			t.Errorf("an install line does not ask for @latest: %s", line)
			continue
		}
		modules[module] = true
	}
	for _, m := range []string{"github.com/snodra/snodra-messaging-go", "github.com/snodra/snodra-messaging-go/observability"} {
		if !modules[m] {
			t.Errorf("the README has no install line for %s", m)
		}
	}
}

// snippets are the regions between "// snippet:begin <name>" and "// snippet:end" of the
// example files, one tab of indentation removed.
func snippets(t *testing.T, path string) []string {
	t.Helper()
	var out []string
	var cur []string
	in := false
	for _, line := range strings.Split(readText(t, path), "\n") {
		switch trimmed := strings.TrimSpace(line); {
		case strings.HasPrefix(trimmed, "// snippet:begin"):
			in, cur = true, nil
		case trimmed == "// snippet:end":
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

// Every Go block of the README is a region of example_test.go (or observability/example_test.go),
// which the compiler checks. The other regions are the site's Go pages' snippets.
func TestEveryReadmeSnippetIsCompiled(t *testing.T) {
	known := map[string]bool{}
	for _, c := range append(snippets(t, "example_test.go"), snippets(t, "observability/example_test.go")...) {
		known[strings.TrimSpace(c)] = true
	}
	blocks := goBlocks(readText(t, "README.md"))
	if len(blocks) == 0 {
		t.Fatal("the README has no Go block")
	}
	for _, b := range blocks {
		if !known[strings.TrimSpace(b)] {
			t.Errorf("a README snippet is not compiled by an example test:\n%s", b)
		}
	}
}
