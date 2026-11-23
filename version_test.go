package snodra

import (
	"os"
	"regexp"
	"testing"
)

func TestClientVersion(t *testing.T) { is(t, clientVersion(), "snodra-messaging-go/"+Version) }

// Version is a release version, written by scripts/bump-version.sh. The release job runs this test
// with SNODRA_RELEASE_VERSION set to the version it is about to tag, so a tag never carries another.
func TestVersion(t *testing.T) {
	if !regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`).MatchString(Version) {
		t.Errorf("Version %q is not N.N.N", Version)
	}
	if want := os.Getenv("SNODRA_RELEASE_VERSION"); want != "" && Version != want {
		t.Errorf("Version is %s, the release is %s", Version, want)
	}
}
