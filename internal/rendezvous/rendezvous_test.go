package rendezvous_test

import (
	"fmt"
	"testing"

	"github.com/snodra/snodra-messaging-go/internal/rendezvous"
)

func is[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// The test vectors every official library shares.
func TestScoreVectors(t *testing.T) {
	is(t, rendezvous.Score("", ""), uint64(0xb9034ad37056f5fb))
	is(t, rendezvous.Score("hub-1", "order-42"), uint64(0x5c855509f5ad7c8d))
	is(t, rendezvous.Score("hub-2", "order-42"), uint64(0xd109ca177e04ae14))
	is(t, rendezvous.Rank([]string{"hub-1", "hub-2"}, "order-42")[0], "hub-2")
	is(t, rendezvous.Rank([]string{"hub-2", "hub-1"}, "order-42")[0], "hub-2")
}

// The zero byte between the id and the key keeps ("ab", "c") and ("a", "bc") apart.
func TestTheSeparatorKeepsPairsApart(t *testing.T) {
	if rendezvous.Score("ab", "c") == rendezvous.Score("a", "bc") {
		t.Fatal("(ab, c) and (a, bc) score the same")
	}
	if rendezvous.Score("a", "") == rendezvous.Score("", "a") {
		t.Fatal("(a, ) and (, a) score the same")
	}
}

// Scores are compared as unsigned 64-bit numbers: a key whose two scores straddle 1<<63 goes to
// the one at or above it, where a signed comparison would pick the other.
func TestRankComparesUnsigned(t *testing.T) {
	ids := []string{"hub-1", "hub-2"}
	for n := range 1000 {
		key := fmt.Sprintf("k-%d", n)
		s1, s2 := rendezvous.Score("hub-1", key), rendezvous.Score("hub-2", key)
		high, low := "hub-1", "hub-2"
		if s2 > s1 {
			high, low = low, high
		}
		if (s1 >= 1<<63) == (s2 >= 1<<63) {
			continue
		}
		ranked := rendezvous.Rank(ids, key)
		is(t, ranked[0], high)
		is(t, ranked[1], low)
		return
	}
	t.Fatal("no key among k-0..k-999 has scores on both sides of 1<<63")
}

// Equal scores (only one id twice can give them) go to the smaller hub id, compared as bytes.
func TestEqualScoresGoToTheSmallerHubID(t *testing.T) {
	is(t, rendezvous.Less("hub-1", 7, "hub-2", 7), true)
	is(t, rendezvous.Less("hub-2", 7, "hub-1", 7), false)
	is(t, rendezvous.Less("hub-2", 8, "hub-1", 7), true)
	is(t, rendezvous.Less("hub-1", 1<<63, "hub-2", 1), true)
	ranked := rendezvous.Rank([]string{"hub-b", "hub-a", "hub-a"}, "k")
	is(t, len(ranked), 3)
}

// A key keeps its hub while the hubs are stable, and only the keys of a hub that leaves move.
func TestOnlyTheKeysOfALeavingHubMove(t *testing.T) {
	three := []string{"hub-a", "hub-b", "hub-c"}
	two := []string{"hub-a", "hub-c"}
	owned := map[string]int{}
	for n := range 1000 {
		key := fmt.Sprintf("order-%d", n)
		before := rendezvous.Rank(three, key)
		owned[before[0]]++
		after := rendezvous.Rank(two, key)[0]
		if before[0] == "hub-b" {
			is(t, after, before[1]) // the runner-up takes over
		} else {
			is(t, after, before[0])
		}
	}
	for _, id := range three {
		if owned[id] < 200 || owned[id] > 470 {
			t.Fatalf("%s owns %d of 1000 keys", id, owned[id])
		}
	}
}
