package main

import (
	"encoding/json"
	"errors"
)

// The service's message types (the conformance contract, "The service's types"). Payloads the
// contract leaves open ("any JSON value") embed json.RawMessage, so they decode whole.

// Echo answers what it got (command).
type Echo struct{ json.RawMessage }

// MessageType is the contract's type name.
func (Echo) MessageType() string { return "conformance.Echo" }

// EchoQuery is Echo as a query.
type EchoQuery struct{ json.RawMessage }

// MessageType is the contract's type name.
func (EchoQuery) MessageType() string { return "conformance.EchoQuery" }

// Done is a command without a result.
type Done struct{ json.RawMessage }

// MessageType is the contract's type name.
func (Done) MessageType() string { return "conformance.Done" }

// Sleep sleeps Ms milliseconds, then answers (command).
type Sleep struct {
	Ms int64 `json:"ms"`
}

// MessageType is the contract's type name.
func (Sleep) MessageType() string { return "conformance.Sleep" }

// SleepQuery is Sleep as a query.
type SleepQuery struct {
	Ms int64 `json:"ms"`
}

// MessageType is the contract's type name.
func (SleepQuery) MessageType() string { return "conformance.SleepQuery" }

// Fail fails as a handler raising an application error.
type Fail struct {
	Message string          `json:"message"`
	Details json.RawMessage `json:"details,omitempty"`
}

// MessageType is the contract's type name.
func (Fail) MessageType() string { return "conformance.Fail" }

// Strict has a typed payload: anything that does not decode to an integer count is the
// library's INVALID_REQUEST.
type Strict struct {
	Count int64 `json:"count"`
}

// MessageType is the contract's type name.
func (Strict) MessageType() string { return "conformance.Strict" }

// UnmarshalJSON requires an object with an integer count, as the reference service does: Go's
// decoder alone would take {} and null as count 0.
func (s *Strict) UnmarshalJSON(b []byte) error {
	n, err := requiredCount(b)
	s.Count = n
	return err
}

// Guarded is Strict for an authenticated caller only.
type Guarded struct {
	Count int64 `json:"count"`
}

// MessageType is the contract's type name.
func (Guarded) MessageType() string { return "conformance.Guarded" }

// UnmarshalJSON is Strict's.
func (g *Guarded) UnmarshalJSON(b []byte) error {
	n, err := requiredCount(b)
	g.Count = n
	return err
}

// requiredCount reads {"count": <integer>} and nothing else: a missing or null count, a
// non-object and a non-integer are errors.
func requiredCount(b []byte) (int64, error) {
	var v struct {
		Count *int64 `json:"count"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return 0, err
	}
	if v.Count == nil {
		return 0, errors.New("count is required")
	}
	return *v.Count, nil
}

// View asks the instance for its own view of its connections (query).
type View struct{ json.RawMessage }

// MessageType is the contract's type name.
func (View) MessageType() string { return "conformance.View" }

// Relay sends Payload on to the runner's Probe (Target "probe") or Missing ("missing").
type Relay struct {
	Target  string          `json:"target"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// MessageType is the contract's type name.
func (Relay) MessageType() string { return "conformance.Relay" }

// Send makes the library send a request (the contract's "conformance.Send").
type Send struct {
	Op            string            `json:"op"`
	Target        string            `json:"target"`
	Payload       json.RawMessage   `json:"payload,omitempty"`
	RoutingKey    *string           `json:"routingKey,omitempty"`
	TimeoutMs     *int64            `json:"timeoutMs,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	Retry         *SendRetry        `json:"retry,omitempty"`
	Async         bool              `json:"async,omitempty"`
	InitialCredit *int64            `json:"initialCredit,omitempty"`
	MaxResults    *int64            `json:"maxResults,omitempty"`
	Read          *int64            `json:"read,omitempty"`
	ReadDelayMs   *int64            `json:"readDelayMs,omitempty"`
}

// SendRetry is a retry policy for one conformance.Send.
type SendRetry struct {
	MaxAttempts      int    `json:"maxAttempts"`
	InitialBackoffMs *int64 `json:"initialBackoffMs,omitempty"`
}

// MessageType is the contract's type name.
func (Send) MessageType() string { return "conformance.Send" }

// Outcome asks for the outcome of an asynchronous conformance.Send.
type Outcome struct {
	OpID   string `json:"opId"`
	WaitMs int64  `json:"waitMs,omitempty"`
}

// MessageType is the contract's type name.
func (Outcome) MessageType() string { return "conformance.Outcome" }

// SharedQuery is the contract's shared query type.
type SharedQuery struct{ json.RawMessage }

// MessageType is the contract's type name.
func (SharedQuery) MessageType() string { return "conformance.SharedQuery" }

// Stream is the contract's streaming query: Count items {n: 1..Count}, each after DelayMs.
type Stream struct {
	Count   int64 `json:"count"`
	DelayMs int64 `json:"delayMs"`
}

// MessageType is the contract's type name.
func (Stream) MessageType() string { return "conformance.Stream" }

// StreamItem is an item of Stream.
type StreamItem struct {
	N int64 `json:"n"`
}

// Watch is the contract's subscription query: by Topic.
type Watch struct {
	Topic string `json:"topic"`
}

// MessageType is the contract's type name.
func (Watch) MessageType() string { return "conformance.Watch" }

// WatchInitial is the initial result of Watch.
type WatchInitial struct {
	Topic      string `json:"topic"`
	Value      int    `json:"value"`
	InstanceID string `json:"instanceId"`
}

// WatchUpdate is an update of Watch: any JSON value.
type WatchUpdate struct {
	Topic string          `json:"topic"`
	Value json.RawMessage `json:"value"`
}

// Emit emits each of Values to every Watch of Topic this instance serves, then completes them
// (Complete) or fails them (Fail, a message).
type Emit struct {
	Topic    string            `json:"topic"`
	Values   []json.RawMessage `json:"values,omitempty"`
	Complete bool              `json:"complete,omitempty"`
	Fail     *string           `json:"fail,omitempty"`
}

// MessageType is the contract's type name.
func (Emit) MessageType() string { return "conformance.Emit" }

// The runner's types, sent by conformance.Send and Relay; their payload passes through as it
// is (the runner scripts its answers with it).

// Probe is the runner's command.
type Probe struct{ json.RawMessage }

// MessageType is the contract's type name.
func (Probe) MessageType() string { return "conformance.runner.Probe" }

// ProbeQuery is the runner's query.
type ProbeQuery struct{ json.RawMessage }

// MessageType is the contract's type name.
func (ProbeQuery) MessageType() string { return "conformance.runner.ProbeQuery" }

// ProbeStream is the runner's streaming query.
type ProbeStream struct{ json.RawMessage }

// MessageType is the contract's type name.
func (ProbeStream) MessageType() string { return "conformance.runner.ProbeStream" }

// ProbeWatch is the runner's subscription query.
type ProbeWatch struct{ json.RawMessage }

// MessageType is the contract's type name.
func (ProbeWatch) MessageType() string { return "conformance.runner.ProbeWatch" }

// Missing is a command nobody serves.
type Missing struct{ json.RawMessage }

// MessageType is the contract's type name.
func (Missing) MessageType() string { return "conformance.runner.Missing" }

// RelayAnswer is the answer of Relay.
type RelayAnswer struct {
	Reply json.RawMessage `json:"reply"`
}

// EchoAnswer is the answer of Echo and EchoQuery.
type EchoAnswer struct {
	Payload    json.RawMessage   `json:"payload"`
	Headers    map[string]string `json:"headers"`
	InstanceID string            `json:"instanceId"`
	RequestID  string            `json:"requestId,omitempty"`
}

// SleepAnswer is the answer of Sleep and SleepQuery.
type SleepAnswer struct {
	InstanceID string `json:"instanceId"`
	SleptMs    int64  `json:"sleptMs"`
}

// SharedQueryAnswer is the answer of SharedQuery: who answered.
type SharedQueryAnswer struct {
	Service    string `json:"service"`
	InstanceID string `json:"instanceId"`
}

// StrictAnswer is the answer of Strict.
type StrictAnswer struct {
	Count int64 `json:"count"`
}

// GuardedAnswer is the answer of Guarded: the count and the caller's sub.
type GuardedAnswer struct {
	Count int64  `json:"count"`
	User  string `json:"user"`
}

// ViewAnswer is the answer of View.
type ViewAnswer struct {
	Service             string   `json:"service"`
	InstanceID          string   `json:"instanceId"`
	HubIDs              []string `json:"hubIds"`
	LastRejection       *string  `json:"lastRejection"`
	SubscriptionsServed int      `json:"subscriptionsServed"`
	Retry               bool     `json:"retry"`
}
