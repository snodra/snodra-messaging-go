package main

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

// State asks what this instance has delivered. Markers names event ids whose processed-event
// marker the instance's store is asked to report.
type State struct {
	Markers []string `json:"markers"`
}

func (State) MessageType() string { return "orders.interop.State" }

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

// StateReply is the instance's view: its identity, the names of its stores and subscription, what
// it delivered, and the processed markers the driver asked about.
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
}

func (OrderPlaced) MessageType() string { return eventType }
