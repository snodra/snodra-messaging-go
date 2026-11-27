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

// rustEventStoreL3Checks is a literal copy of the level-3 list in the Rust event-store
// conformance kit, in run_event_store_conformance's order, pinned on 2026-11-27.
var rustEventStoreL3Checks = []string{
	"replay_from_earliest_redelivers_history",
	"admin_without_replay_is_unsupported",
	"lag_counts_unacked",
	"lag_unknown_without_capability",
	"dead_letter_source_receives",
	"search_finds_by_type_key_and_correlation",
	"search_stops_at_bounds",
	"tail_streams_new_events",
	"browser_never_commits_a_position",
	"no_browser_without_capability",
}

// rustEventStoreBrowseChecks are the four level-3 checks the Go kit omits: the Go library has no
// event browser. The plan records this as a kit difference.
var rustEventStoreBrowseChecks = []string{
	"search_finds_by_type_key_and_correlation",
	"search_stops_at_bounds",
	"tail_streams_new_events",
	"browser_never_commits_a_position",
}

// withoutChecks is list without the entries of omit.
func withoutChecks(list, omit []string) []string {
	kept := make([]string, 0, len(list))
	for _, id := range list {
		if slices.Contains(omit, id) {
			continue
		}
		kept = append(kept, id)
	}
	return kept
}

// TestChecksTxtMatchesTheRustKit pins the published check list to the Rust kit's level-2 list and
// its level-3 list without the four browse checks (a kit difference).
func TestChecksTxtMatchesTheRustKit(t *testing.T) {
	data, err := os.ReadFile("checks.txt")
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Fields(string(data))
	wantL3 := withoutChecks(rustEventStoreL3Checks, rustEventStoreBrowseChecks)
	want := append(append([]string(nil), rustEventStoreL2Checks...), wantL3...)
	if !slices.Equal(got, want) {
		t.Fatalf("checks.txt has %v, want the Rust kit's list %v", got, want)
	}
	if ids := CheckIDs(); !slices.Equal(ids, want) {
		t.Fatalf("the kit lists %v, want the Rust kit's list %v", ids, want)
	}
	if len(rustEventStoreL2Checks) != 9 {
		t.Fatalf("the level-2 kit has %d checks, want 9", len(rustEventStoreL2Checks))
	}
	if len(rustEventStoreL3Checks) != 10 || len(rustEventStoreBrowseChecks) != 4 || len(wantL3) != 6 {
		t.Fatalf("the level-3 lists: %d, %d, %d", len(rustEventStoreL3Checks), len(rustEventStoreBrowseChecks), len(wantL3))
	}
}

// TestSpecNamesCarryOneProcessSuffix pins that a run's subscription names never reuse a stored
// committed position of an earlier run.
func TestSpecNamesCarryOneProcessSuffix(t *testing.T) {
	if processSuffix == "" {
		t.Fatal("the process suffix is empty")
	}
	kit := &EventStoreKit{Source: "conformance.events"}
	first, second := kit.spec(), kit.spec()
	prefix := "conformance." + processSuffix + "-"
	for _, name := range []string{first.Name, second.Name} {
		if !strings.HasPrefix(name, prefix) {
			t.Fatalf("subscription name %q does not start with %q", name, prefix)
		}
	}
	if first.Name == second.Name {
		t.Fatalf("two subscriptions share the name %q", first.Name)
	}
}
