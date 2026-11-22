package uuid_test

import (
	"regexp"
	"testing"

	"github.com/snodra/snodra-messaging-go/internal/uuid"
)

func TestNewIsARandomV4UUID(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	seen := map[string]bool{}
	for range 1000 {
		u := uuid.New()
		is(t, re.MatchString(u), true)
		seen[u] = true
	}
	is(t, len(seen), 1000)
}
