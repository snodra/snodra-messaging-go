package snodratest_test

import (
	"testing"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/snodratest"
)

// The level-2, level-3 and level-4 message-store checks on the in-memory message store.
func TestMemoryMessageStorePassesTheKit(t *testing.T) {
	snodratest.MessageStore(t, func() snodra.MessageStore {
		return snodra.NewMemoryMessageStore()
	})
}
