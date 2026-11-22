package main

import "encoding/json"

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

// View asks the instance for its own view of its connections (query).
type View struct{ json.RawMessage }

// MessageType is the contract's type name.
func (View) MessageType() string { return "conformance.View" }

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

// StrictAnswer is the answer of Strict.
type StrictAnswer struct {
	Count int64 `json:"count"`
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
