package snodratest

import (
	"os"
	"slices"
	"strings"
	"testing"
)

// rustMessageStoreL2Checks is a literal copy of the level-2 store list in the Rust message-store
// conformance kit's run_store_conformance (snodra-messaging-rs, src/store/conformance.rs), pinned
// on 2026-11-26, with delete_markers_removes_only_the_range (level 3) omitted. The Rust list is
// authoritative; when it changes, this copy and store_checks.txt change with it.
var rustMessageStoreL2Checks = []string{
	"appended_entry_is_claimed_in_created_seq_order",
	"outbox_rejects_a_payload_that_is_not_a_json_object",
	"pending_counts_entries_neither_sent_nor_failed",
	"rolled_back_transaction_leaves_no_outbox_entry",
	"rolled_back_handler_leaves_no_processed_marker",
	"processed_marker_records_once",
	"concurrent_relays_publish_each_entry_once",
	"expired_lease_is_reclaimed",
	"purge_removes_only_sent_before",
}

// TestStoreChecksTxtMatchesTheRustKit pins the checked-in message-store list to the Rust kit's
// level-2 list.
func TestStoreChecksTxtMatchesTheRustKit(t *testing.T) {
	data, err := os.ReadFile("store_checks.txt")
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Fields(string(data))
	if !slices.Equal(got, rustMessageStoreL2Checks) {
		t.Fatalf("store_checks.txt has %v, want the Rust kit's level-2 list %v", got, rustMessageStoreL2Checks)
	}
	if ids := StoreCheckIDs(); !slices.Equal(ids, rustMessageStoreL2Checks) {
		t.Fatalf("the kit runs %v, want the Rust kit's level-2 list %v", ids, rustMessageStoreL2Checks)
	}
	if len(rustMessageStoreL2Checks) != 9 {
		t.Fatalf("the level-2 store kit has %d checks, want 9", len(rustMessageStoreL2Checks))
	}
}
