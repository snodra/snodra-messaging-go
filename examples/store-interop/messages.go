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

// The saga interop's message types. They are neutral orders shapes like the rest of the probe;
// the same saga type and state are registered in both languages, so one shared store carries an
// instance a Go and a Python service continue.
const (
	// SagaType is the registered saga name and the saga_* marker prefix both languages share.
	SagaType = "interop-saga"
	// SagaAssociationKey is the association both languages route the saga events by; the value is
	// the order id.
	SagaAssociationKey = "orderId"
	// SagaDeadlineName is the deadline the start handler schedules.
	SagaDeadlineName = "saga-timeout"
	// SagaStartedType, SagaAdvancedType are the event types the driver publishes.
	SagaStartedType  = "orders.interop.SagaStarted"
	SagaAdvancedType = "orders.interop.SagaAdvanced"
	// SagaFailType is the command whose handler fails permanently (INVALID), producing a notice.
	SagaFailType = "orders.interop.SagaFail"
	// SagaSlowType is the command whose handler is slow, holding the dispatch lease.
	SagaSlowType = "orders.interop.SagaSlow"
	// SagaSlowStartedType is the command the slow handler sends to the driver's probe service.
	SagaSlowStartedType = "store.runner.SlowStarted"
	// sagaCommandFailedType is the library's local failure notice type; it must never be published.
	sagaCommandFailedType = "snodra.SagaCommandFailed"
)

// SagaStarted starts a saga; Mode selects what the later advanced event makes the saga do
// ("continue", "lease", "notice" or "deadline").
type SagaStarted struct {
	OrderID string `json:"orderId"`
	Run     string `json:"run"`
	Family  string `json:"family"`
	Mode    string `json:"mode"`
}

func (SagaStarted) MessageType() string { return SagaStartedType }

// SagaAdvanced continues a live saga; the driver publishes it on the other language's partition.
type SagaAdvanced struct {
	OrderID string `json:"orderId"`
	Run     string `json:"run"`
	Family  string `json:"family"`
	Mode    string `json:"mode"`
}

func (SagaAdvanced) MessageType() string { return SagaAdvancedType }

// SagaDeadlinePayload is what the start handler schedules and the deadline handler receives.
type SagaDeadlinePayload struct {
	OrderID string `json:"orderId"`
	Run     string `json:"run"`
	Family  string `json:"family"`
}

// SagaFail is the command whose handler returns INVALID, so the saga receives a local notice.
type SagaFail struct {
	SagaID  string `json:"sagaId"`
	OrderID string `json:"orderId"`
}

func (SagaFail) MessageType() string { return SagaFailType }

// SagaFailReply is the command's answer; the handler never returns it.
type SagaFailReply struct{}

// SagaSlow is the command whose handler holds the dispatch lease while it works.
type SagaSlow struct {
	SagaID  string `json:"sagaId"`
	OrderID string `json:"orderId"`
}

func (SagaSlow) MessageType() string { return SagaSlowType }

// SagaSlowReply is the slow command's answer.
type SagaSlowReply struct{}

// SagaSlowStarted is what the slow command sends to the driver: which language's instance ran it.
type SagaSlowStarted struct {
	SagaID   string `json:"sagaId"`
	OrderID  string `json:"orderId"`
	Language string `json:"language"`
}

func (SagaSlowStarted) MessageType() string { return SagaSlowStartedType }

// InteropSagaState is the shared saga state. Both languages use the same fields and wire names, so
// either can continue an instance the other created.
type InteropSagaState struct {
	OrderID             string `json:"orderId"`
	Run                 string `json:"run"`
	Family              string `json:"family"`
	Mode                string `json:"mode"`
	StartedBy           string `json:"startedBy"`
	AdvancedBy          string `json:"advancedBy"`
	Advances            int    `json:"advances"`
	DeadlineScheduledBy string `json:"deadlineScheduledBy"`
	DeadlineFired       bool   `json:"deadlineFired"`
	DeadlineFiredBy     string `json:"deadlineFiredBy"`
	SlowRecordedBy      string `json:"slowRecordedBy"`
	NoticeBy            string `json:"noticeBy"`
	NoticeReason        string `json:"noticeReason"`
	NoticeCommandType   string `json:"noticeCommandType"`
	NoticeKey           string `json:"noticeKey"`
	NoticeMessage       string `json:"noticeMessage"`
	Notices             int    `json:"notices"`
}

// SagaPublish publishes one saga event through the instance's event store, on the key the driver
// chose: the partition owner consumes it and runs stage 2.
type SagaPublish struct {
	Family  string `json:"family"`
	Run     string `json:"run"`
	ID      string `json:"id"`
	Key     string `json:"key"`
	Kind    string `json:"kind"` // "started" or "advanced"
	OrderID string `json:"orderId"`
	Mode    string `json:"mode"`
}

func (SagaPublish) MessageType() string { return "orders.interop.SagaPublish" }

// SagaPublishReply names the id the event store accepted.
type SagaPublishReply struct {
	Instance  string `json:"instance"`
	Published string `json:"published"`
}

// SagaStateRequest reads a saga by id, or the live instance associated with Key = Value.
type SagaStateRequest struct {
	Family string `json:"family"`
	Run    string `json:"run"`
	Type   string `json:"type"`
	Key    string `json:"key"`
	Value  string `json:"value"`
	SagaID string `json:"sagaId"`
}

func (SagaStateRequest) MessageType() string { return "orders.interop.SagaState" }

// SagaStateReply is one saga instance as the shared store holds it; State is the decoded JSON.
type SagaStateReply struct {
	Instance     string                `json:"instance"`
	Found        bool                  `json:"found"`
	SagaID       string                `json:"sagaId,omitempty"`
	Type         string                `json:"type,omitempty"`
	State        json.RawMessage       `json:"state,omitempty"`
	Version      int64                 `json:"version"`
	Ended        bool                  `json:"ended"`
	Live         int                   `json:"live"`
	Associations []SagaAssociationView `json:"associations,omitempty"`
}

// SagaAssociationView is one association of a saga instance.
type SagaAssociationView struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// SagaDispatchRequest reads the first pending dispatch entry and the pending count of a saga.
type SagaDispatchRequest struct {
	Family string `json:"family"`
	Run    string `json:"run"`
	SagaID string `json:"sagaId"`
}

func (SagaDispatchRequest) MessageType() string { return "orders.interop.SagaDispatch" }

// SagaDispatchReply is the pending entry and the count; Pending is false when none.
type SagaDispatchReply struct {
	Instance     string `json:"instance"`
	Pending      bool   `json:"pending"`
	EntryID      string `json:"entryId,omitempty"`
	Seq          int64  `json:"seq"`
	Kind         string `json:"kind,omitempty"`
	Type         string `json:"type,omitempty"`
	Attempts     int    `json:"attempts"`
	PendingCount int64  `json:"pendingCount"`
}

// SagaLeaseRequest tries to take a saga's dispatch lease, as another relay would.
type SagaLeaseRequest struct {
	Family  string `json:"family"`
	Run     string `json:"run"`
	SagaID  string `json:"sagaId"`
	Owner   string `json:"owner"`
	UntilMs int64  `json:"untilMs"`
}

func (SagaLeaseRequest) MessageType() string { return "orders.interop.SagaLease" }

// SagaLeaseReply says whether the probe owner got the lease.
type SagaLeaseReply struct {
	Instance string `json:"instance"`
	Taken    bool   `json:"taken"`
}

// SagaReleaseRequest releases a probe owner's lease.
type SagaReleaseRequest struct {
	Family string `json:"family"`
	Run    string `json:"run"`
	SagaID string `json:"sagaId"`
	Owner  string `json:"owner"`
}

func (SagaReleaseRequest) MessageType() string { return "orders.interop.SagaRelease" }

// SagaReleaseReply says whether the probe owner's lease was released.
type SagaReleaseReply struct {
	Instance string `json:"instance"`
	Released bool   `json:"released"`
}

// SagaNoticeRequest reads a probe instance's local view of one saga's failure notice.
type SagaNoticeRequest struct {
	Family string `json:"family"`
	Run    string `json:"run"`
	SagaID string `json:"sagaId"`
}

func (SagaNoticeRequest) MessageType() string { return "orders.interop.SagaNotice" }

// SagaNoticeReply says how many ordinary notices this instance delivered for the saga and how many
// event-store deliveries the notice id has: the notice is local, so the latter is zero.
type SagaNoticeReply struct {
	Instance  string `json:"instance"`
	Language  string `json:"language"`
	Ordinary  int    `json:"ordinary"`
	Published int    `json:"published"`
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
	Instance        string          `json:"instance"`
	Language        string          `json:"language"`
	Family          string          `json:"family"`
	Run             string          `json:"run"`
	Store           string          `json:"store"`
	EventStore      string          `json:"eventStore"`
	Source          string          `json:"source"`
	Database        string          `json:"database,omitempty"`
	Subscription    string          `json:"subscription"`
	OutboxPending   int64           `json:"outboxPending"`
	Handled         int             `json:"handled"`
	Duplicates      int             `json:"duplicates"`
	OptedOut        uint64          `json:"optedOut"`
	Paused          bool            `json:"paused"`
	PausedBy        string          `json:"pausedBy,omitempty"`
	OperationID     string          `json:"operationId,omitempty"`
	Replay          []ReplayRange   `json:"replay,omitempty"`
	DeadLetters     []LetterBrief   `json:"deadLetters,omitempty"`
	DeadLetterTotal int64           `json:"deadLetterTotal"`
	Dlt             []DltEvent      `json:"dlt,omitempty"`
	DltTotal        uint64          `json:"dltTotal"`
	Deliveries      []Delivery      `json:"deliveries"`
	Markers         map[string]bool `json:"markers"`
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
