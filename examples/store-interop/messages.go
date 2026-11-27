package main

import "encoding/json"

// The probe's wire messages. They are commands (a command answers, so the driver can address one
// instance with a routing key); the shapes are the neutral orders-service naming the rest of the
// examples use.

// Who is answered with the instance, language and family that serves it: the driver uses it to
// find a routing key per instance.
type Who struct{}

func (Who) MessageType() string { return "orders.interop.Who" }

// WhoReply says which probe answered.
type WhoReply struct {
	Instance string `json:"instance"`
	Language string `json:"language"`
	Family   string `json:"family"`
}

// Publish appends Count events to this instance's outbox, cycling through Keys: the app's relay
// publishes them to the family's event store afterwards.
type Publish struct {
	Family    string   `json:"family"`
	Publisher string   `json:"publisher"`
	Run       string   `json:"run"`
	Count     int      `json:"count"`
	Keys      []string `json:"keys"`
}

func (Publish) MessageType() string { return "orders.interop.Publish" }

// PublishReply names the ids of the appended outbox entries.
type PublishReply struct {
	Instance string   `json:"instance"`
	IDs      []string `json:"ids"`
}

// PublishRaw publishes events through the event store directly, in this instance's process: the
// driver uses it to place an event on a chosen partition, the outbox relay not being involved.
type PublishRaw struct {
	Family    string   `json:"family"`
	Publisher string   `json:"publisher"`
	Run       string   `json:"run"`
	IDs       []string `json:"ids"`
	Keys      []string `json:"keys"`
}

func (PublishRaw) MessageType() string { return "orders.interop.PublishRaw" }

// PublishRawReply names the ids the event store accepted.
type PublishRawReply struct {
	Instance  string   `json:"instance"`
	Published []string `json:"published"`
}

// State asks what this instance has delivered, what its processor's control state is, and about
// the processed markers of Markers.
type State struct {
	Markers []string `json:"markers"`
}

func (State) MessageType() string { return "orders.interop.State" }

// ControlSet pauses or resumes this instance's processor by writing the message store's processor
// control state directly: the driver uses it to prove that a pause one language persists is seen
// and obeyed by the other, before the hub's own control calls run.
type ControlSet struct {
	Family string `json:"family"`
	Run    string `json:"run"`
	Paused bool   `json:"paused"`
	By     string `json:"by"`
}

func (ControlSet) MessageType() string { return "orders.interop.ControlSet" }

// ControlReply is the processor's control state after a ControlSet, and the instance that wrote
// it.
type ControlReply struct {
	Instance string `json:"instance"`
	Paused   bool   `json:"paused"`
	PausedBy string `json:"pausedBy"`
}

// Poison publishes one event its handler rejects until the driver allows the event id: it parks
// a letter in mode collection, or reaches <source>.dlt in mode event-store, and a later retry
// after Allow, or the other language's service, can then handle it.
type Poison struct {
	Family    string `json:"family"`
	Publisher string `json:"publisher"`
	Run       string `json:"run"`
	ID        string `json:"id"`
	Key       string `json:"key"`
}

func (Poison) MessageType() string { return "orders.interop.Poison" }

// PoisonReply names the id the event store accepted.
type PoisonReply struct {
	Instance  string `json:"instance"`
	Published string `json:"published"`
}

// Allow lets this instance's handlers take the poison events of IDs: the driver calls it on
// every instance of the family before a retry, so whichever the hub picks can handle the record.
type Allow struct {
	Family string   `json:"family"`
	Run    string   `json:"run"`
	IDs    []string `json:"ids"`
}

func (Allow) MessageType() string { return "orders.interop.Allow" }

// AllowReply names the instance and how many ids it now allows.
type AllowReply struct {
	Instance string `json:"instance"`
	Allowed  int    `json:"allowed"`
}

// ReplayRange is one open replay range of the processor's control state.
type ReplayRange struct {
	Segment string `json:"segment"`
	From    string `json:"from"`
	Until   string `json:"until"`
}

// LetterBrief is one dead letter as this instance's store lists it.
type LetterBrief struct {
	ID       string `json:"id"`
	Reason   string `json:"reason"`
	EventID  string `json:"eventId"`
	Attempts int    `json:"attempts"`
}

// DltEvent is one event the probe's dead-letter subscription received: the event-store mode
// publishes a record no handler could take to <source>.dlt.
type DltEvent struct {
	ID       string            `json:"id"`
	Type     string            `json:"type"`
	Key      string            `json:"key"`
	Segment  string            `json:"segment"`
	Position string            `json:"position"`
	Headers  map[string]string `json:"headers"`
	Payload  json.RawMessage   `json:"payload"`
}

// Delivery is one event delivery this instance's consumer recorded a stage-1 marker for:
// "handled" when the handler ran in the same transaction, "duplicate" when the marker existed
// already (the handler did not run).
type Delivery struct {
	ID        string `json:"id"`
	Segment   string `json:"segment"`
	Position  string `json:"position"`
	Outcome   string `json:"outcome"`
	Publisher string `json:"publisher,omitempty"`
	Key       string `json:"key,omitempty"`
}

// StateReply is the instance's view: its identity, the names of its stores and subscription, its
// processor's control state, what it delivered, the dead letters its store lists, the dead-letter
// events its probe subscription received, and the processed markers the driver asked about.
type StateReply struct {
	Instance      string          `json:"instance"`
	Language      string          `json:"language"`
	Family        string          `json:"family"`
	Run           string          `json:"run"`
	Store         string          `json:"store"`
	EventStore    string          `json:"eventStore"`
	Source        string          `json:"source"`
	Database      string          `json:"database,omitempty"`
	Subscription  string          `json:"subscription"`
	OutboxPending int64           `json:"outboxPending"`
	Handled       int             `json:"handled"`
	Duplicates    int             `json:"duplicates"`
	OptedOut      uint64          `json:"optedOut"`
	Paused        bool            `json:"paused"`
	PausedBy      string          `json:"pausedBy,omitempty"`
	OperationID   string          `json:"operationId,omitempty"`
	Replay        []ReplayRange   `json:"replay,omitempty"`
	DeadLetters   []LetterBrief   `json:"deadLetters,omitempty"`
	DeadLetterN   int64           `json:"deadLetterTotal"`
	Dlt           []DltEvent      `json:"dlt,omitempty"`
	DltTotal      uint64          `json:"dltTotal"`
	Deliveries    []Delivery      `json:"deliveries"`
	Markers       map[string]bool `json:"markers"`
}

// OrderPlaced is the event the probe publishes. It carries everything a driver assertion reads,
// so a handler never needs the event store's own metadata.
type OrderPlaced struct {
	ID        string `json:"id"`
	Run       string `json:"run"`
	Publisher string `json:"publisher"`
	Family    string `json:"family"`
	Key       string `json:"key"`
	Seq       int64  `json:"seq"`
	// Poison marks an event the handler rejects until the driver allows its id.
	Poison bool `json:"poison,omitempty"`
}

func (OrderPlaced) MessageType() string { return eventType }
