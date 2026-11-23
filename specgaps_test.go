package snodra

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Every // TODO(spec-gap): SG-n in the code has its entry in SPEC-GAPS.md, and every entry its
// marker in the code.
func TestSpecGapsAndTheirMarkersAgree(t *testing.T) {
	doc, err := os.ReadFile("SPEC-GAPS.md")
	if err != nil {
		t.Fatal(err)
	}
	entries := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?m)^### (SG-\d+):`).FindAllStringSubmatch(string(doc), -1) {
		entries[m[1]] = true
	}
	marker := regexp.MustCompile(`TODO\(spec-gap\)[^\n]*?(SG-\d+)`)
	marked := map[string]bool{}
	err = filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "bin") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, m := range marker.FindAllStringSubmatch(string(src), -1) {
			marked[m[1]] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for id := range marked {
		if !entries[id] {
			t.Errorf("%s is marked in the code and has no entry in SPEC-GAPS.md", id)
		}
	}
	for id := range entries {
		if !marked[id] {
			t.Errorf("%s has an entry in SPEC-GAPS.md and no TODO(spec-gap) marker in the code", id)
		}
	}
	if len(entries) == 0 {
		t.Fatal("no entries read from SPEC-GAPS.md")
	}
}
