// Package rendezvous is the hub choice of a keyed request: the rendezvous score of a hub id and a
// routing key, and the ranking of hubs by it. Every official library uses exactly this function,
// so that a key goes through the same hub whatever language sends it
// (https://snodra.damoreira.com/library-authors/protocol/, Several hubs).
package rendezvous

import (
	"slices"
	"strings"
)

const (
	fnvBasis uint64 = 0xcbf29ce484222325
	fnvPrime uint64 = 0x100000001b3
)

// Score is the rendezvous score of (hubID, key): 64-bit FNV-1a over the UTF-8 bytes of hubID, one
// zero byte and the UTF-8 bytes of key, then MurmurHash3's fmix64 finalizer. The zero byte keeps
// ("ab", "c") and ("a", "bc") apart.
func Score(hubID, key string) uint64 {
	h := fnvBasis
	for i := 0; i < len(hubID); i++ {
		h = (h ^ uint64(hubID[i])) * fnvPrime
	}
	h *= fnvPrime // the zero byte: h ^ 0 is h
	for i := 0; i < len(key); i++ {
		h = (h ^ uint64(key[i])) * fnvPrime
	}
	return fmix64(h)
}

// fmix64 is MurmurHash3's 64-bit finalizer: every input bit affects every output bit.
func fmix64(k uint64) uint64 {
	k ^= k >> 33
	k *= 0xff51afd7ed558ccd
	k ^= k >> 33
	k *= 0xc4ceb9fe1a85ec53
	return k ^ (k >> 33)
}

// Less reports whether the hub a with score sa comes before the hub b with score sb: the higher
// score as an unsigned number first, equal scores by the smaller hub id, compared as bytes.
func Less(a string, sa uint64, b string, sb uint64) bool {
	if sa != sb {
		return sa > sb
	}
	return a < b
}

// Rank orders hubIDs from the preferred to the least preferred hub for key (a new slice).
func Rank(hubIDs []string, key string) []string {
	type scored struct {
		id    string
		score uint64
	}
	all := make([]scored, len(hubIDs))
	for i, id := range hubIDs {
		all[i] = scored{id, Score(id, key)}
	}
	slices.SortStableFunc(all, func(x, y scored) int {
		switch {
		case Less(x.id, x.score, y.id, y.score):
			return -1
		case Less(y.id, y.score, x.id, x.score):
			return 1
		}
		return strings.Compare(x.id, y.id)
	})
	out := make([]string, len(all))
	for i, s := range all {
		out[i] = s.id
	}
	return out
}
