// Package snodratest is the conformance kit for Snodra event-store add-ons. An add-on's own
// test hands it a source it prepared and a way to make the add-on; the kit runs the nine
// level-2 event-store checks as subtests, named exactly as the Rust kit names them.
//
// The kit imports only the standard library and the core package, never a store client, so a
// third-party add-on depends only on the core.
//
// An add-on proves itself with one call:
//
//	func TestMyEventStore(t *testing.T) {
//		store := myeventstore.New()
//		markers := mystore.New(ctx) // any message store, its schema in place
//		snodratest.EventStore(t, "snodra.events",
//			func() snodra.EventStore { return store },
//			markers.Processed())
//	}
//
// The arguments:
//
//   - source has at least two segments (the competing-instances check spreads them over two
//     instances), and when the add-on declares CapabilityNativeDeadLetter its dead-letter
//     source (snodra.DeadLetterSource(source)) exists too. The kit is the source's only
//     publisher while it runs.
//   - open is one service instance per call: an add-on reading the same event store (an
//     in-process store returns the same one each time). Instances compete when they share a
//     subscription name.
//   - markers records the processed-event marker of the at-least-once check: pass the message
//     store's processed-event store. The kit records one marker, the way the consumer does.
//
// The checks run one after the other in the Rust kit's order, each on subscription names of its
// own; a new name starts at the earliest position, so each check reads what the earlier ones
// appended and looks only at its own events (by their id header). The first failing check stops
// the kit and its message names the check and the event store. Every check is exported, so a
// failing one can be run by itself on an EventStoreKit.
//
// Each check waits at most a minute for what the event store does in the background, and for
// every call it makes: a publish, a subscribe or a close that does not answer fails the check
// naming the step. The checks are the nine level-2 checks; the capability checks (replay, lag,
// native dead letter, and the absent-capability checks) come with level 3.
package snodratest
