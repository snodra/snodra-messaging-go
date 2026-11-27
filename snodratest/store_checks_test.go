package snodratest

import (
	"os"
	"slices"
	"strings"
	"testing"
)

// rustMessageStoreGoChecks is a literal copy of the level-2 and level-3 store checks in the Rust
// message-store conformance kit's run_store_conformance (snodra-messaging-rs,
// src/store/conformance.rs), pinned on 2026-11-27, in the Rust order: delete_markers (level 3, the
// Rust list places it among the level-2 checks) and the seven level-3 checks at the end. The Rust
// list is authoritative; when it changes, this copy and store_checks.txt change with it.
var rustMessageStoreGoChecks = []string{
	"appended_entry_is_claimed_in_created_seq_order",
	"outbox_rejects_a_payload_that_is_not_a_json_object",
	"pending_counts_entries_neither_sent_nor_failed",
	"rolled_back_transaction_leaves_no_outbox_entry",
	"rolled_back_handler_leaves_no_processed_marker",
	"processed_marker_records_once",
	"delete_markers_removes_only_the_range",
	"concurrent_relays_publish_each_entry_once",
	"expired_lease_is_reclaimed",
	"purge_removes_only_sent_before",
	"dead_letters_sequence_per_key",
	"dead_letter_parked_inside_rolled_back_transaction_is_kept",
	"dead_letter_deleted_in_a_transaction_goes_with_it",
	"control_lock_is_exclusive_until_lease_expires",
	"pause_state_round_trips",
	"replay_ranges_round_trip",
	"processor_control_write_inside_rolled_back_transaction_is_kept",
}

// TestStoreChecksTxtMatchesTheRustKit pins the checked-in message-store list to the Rust kit's
// level-2 and level-3 checks.
func TestStoreChecksTxtMatchesTheRustKit(t *testing.T) {
	data, err := os.ReadFile("store_checks.txt")
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Fields(string(data))
	if !slices.Equal(got, rustMessageStoreGoChecks) {
		t.Fatalf("store_checks.txt has %v, want the Rust kit's level-2 and level-3 checks %v", got, rustMessageStoreGoChecks)
	}
	if ids := StoreCheckIDs(); !slices.Equal(ids, rustMessageStoreGoChecks) {
		t.Fatalf("the kit runs %v, want the Rust kit's level-2 and level-3 checks %v", ids, rustMessageStoreGoChecks)
	}
	if len(rustMessageStoreGoChecks) != 17 {
		t.Fatalf("the store kit has %d checks, want 17 (9 level-2 and 8 level-3)", len(rustMessageStoreGoChecks))
	}
}
