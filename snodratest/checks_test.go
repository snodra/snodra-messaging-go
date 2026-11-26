package snodratest

import (
	"os"
	"slices"
	"strings"
	"testing"
)

// rustEventStoreL2Checks is a literal copy of the level-2 list in the Rust event-store
// conformance kit's run_event_store_conformance (snodra-messaging-rs,
// src/event_store/conformance.rs), pinned on 2026-11-26. The Rust list is authoritative; when it
// changes, this copy and checks.txt change with it.
var rustEventStoreL2Checks = []string{
	"headers_round_trip",
	"same_key_arrives_in_publish_order",
	"every_processor_gets_every_event",
	"instances_share_a_processor",
	"a_joining_instance_announces_its_first_assignment",
	"an_instance_left_without_segments_closes_its_change",
	"unacked_delivery_is_redelivered",
	"redelivered_after_commit_is_skipped",
	"paused_subscription_receives_nothing_and_resumes_in_place",
}

// TestChecksTxtMatchesTheRustKit pins the published check list to the Rust kit's level-2 list.
func TestChecksTxtMatchesTheRustKit(t *testing.T) {
	data, err := os.ReadFile("checks.txt")
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Fields(string(data))
	if !slices.Equal(got, rustEventStoreL2Checks) {
		t.Fatalf("checks.txt has %v, want the Rust kit's level-2 list %v", got, rustEventStoreL2Checks)
	}
	if ids := CheckIDs(); !slices.Equal(ids, rustEventStoreL2Checks) {
		t.Fatalf("the kit runs %v, want the Rust kit's level-2 list %v", ids, rustEventStoreL2Checks)
	}
	if len(rustEventStoreL2Checks) != 9 {
		t.Fatalf("the level-2 kit has %d checks, want 9", len(rustEventStoreL2Checks))
	}
}
