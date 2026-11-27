// Package snodratest is the conformance kit for Snodra add-ons: the event-store kit
// ([EventStore]) and the message-store kit ([MessageStore]). An add-on's own test hands each kit
// what it needs; the kit runs its checks as subtests, named exactly as the Rust kit names them.
// An add-on that implements both interfaces proves both with two calls.
//
// The kit imports only the standard library and the core package, never a store client, so a
// third-party add-on depends only on the core.
//
// The event-store kit:
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
// The message-store kit:
//
//	func TestMyMessageStore(t *testing.T) {
//		snodratest.MessageStore(t, func() snodra.MessageStore {
//			return mystore.New(ctx) // empty state, its schema ready
//		})
//	}
//
// It runs the nine level-2 outbox and processed-event checks and the eight level-3 control and
// dead-letter checks on a store over empty state. Each check gets its own store (open is called
// once per check), so checks never see each other's data; every check is exported as a function
// taking a testing.TB and a store, so a failing one can be run by itself.
//
// The event-store kit runs the nine level-2 checks and the level-3 capability checks: replay (or
// admin-without-replay), lag (or lag-without-capability), native-dead-letter and the no-browser
// check. The four browse checks are omitted as a kit difference: the Go library has no event
// browser. A store that does not declare browse passes no_browser_without_capability; one that
// declares browse skips it (the check is vacuous there) and the kit is told the browse checks are
// a kit difference it cannot run.
// The message-store kit runs its level-2 and level-3 checks.
//
// The checks run one after the other in the Rust kit's order, each on subscription names (event
// store) or stores (message store) of its own. The first failing check stops the kit and its
// message names the check and the add-on. Every check is exported, so a failing one can be run by
// itself. Each check waits at most a minute for what the add-on does in the background, and for
// every call it makes: a publish, a subscribe, a claim or a close that does not answer fails the
// check naming the step.
//
// The event-store kit has the nine level-2 checks and the level-3 capability checks above; the
// message-store kit has its nine level-2 checks and its eight level-3 checks.
package snodratest
