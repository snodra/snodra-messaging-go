package snodra

import (
	"context"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// The bounds hub.proto puts on a ProcessorStatus (the hub cuts longer strings and drops the
// rest).
const (
	// maxStatusNameBytes is where ProcessorStatus.event_store and each capability end.
	maxStatusNameBytes = 64
	// maxStatusCapabilities is how many capabilities a status lists.
	maxStatusCapabilities = 16
	// maxStatusLastErrorBytes is where ProcessorStatus.last_error ends.
	maxStatusLastErrorBytes = 512
)

// processorStatuses is one ProcessorStatus per processor that started at least once (or holds a
// reset operation), built from memory only: it never waits for the event store, the message
// store or a handler, so it cannot delay a status tick or a consumer. nil when none did. Each
// call also asks the sampler, without waiting, to refresh the dead-letter counts that need the
// store.
// See https://snodra.damoreira.com/docs/library-authors/protocol/ (Processor status).
func (a *App) processorStatuses() []*hubpb.ProcessorStatus {
	if a.cfg.store == nil || a.cfg.eventStore == nil || len(a.processors) == 0 ||
		!a.cfg.events.consumerEnabled {
		return nil
	}
	now := a.now()
	statuses := make([]*hubpb.ProcessorStatus, 0, len(a.processors))
	for _, p := range a.processors {
		if !p.run.hasStarted() && p.run.operation() == "" {
			continue // a processor that never started is not one this instance runs
		}
		statuses = append(statuses, a.processorStatus(p, now))
	}
	if len(statuses) == 0 {
		return nil
	}
	a.kickDeadLetterSample()
	return statuses
}

// processorStatus builds one processor's status from the runtime state now.
func (a *App) processorStatus(p *processor, now time.Time) *hubpb.ProcessorStatus {
	p.run.takeSubscriptionError(now)
	snap := p.run.snapshot()

	state := hubpb.ProcessorState_PROCESSOR_STOPPED
	switch {
	case !snap.started:
	case snap.paused:
		state = hubpb.ProcessorState_PROCESSOR_PAUSED
	case !snap.failingSince.IsZero() && now.Sub(snap.failingSince) < a.cfg.timings.failingWindow:
		state = hubpb.ProcessorState_PROCESSOR_FAILING
	case !snap.assigned || snap.rebalancing:
		state = hubpb.ProcessorState_PROCESSOR_STARTING
	default:
		state = hubpb.ProcessorState_PROCESSOR_RUNNING
	}

	deadLetters := uint64(0)
	if a.cfg.events.deadLetterMode == DeadLetterModeCollection {
		deadLetters = p.deadLetterCount.Load()
	}
	lastErrorAt := int64(0)
	if !snap.lastErrorAt.IsZero() {
		lastErrorAt = snap.lastErrorAt.UnixMilli()
	}
	status := &hubpb.ProcessorStatus{
		Name:           p.name,
		Subscription:   p.subscription.Name,
		Source:         p.subscription.Source,
		State:          state,
		ProcessedTotal: p.processedTotal.Load(),
		FailedTotal:    p.failedTotal.Load(),
		DeadLetters:    deadLetters,
		LastError:      cutUTF8Bytes(snap.lastError, maxStatusLastErrorBytes),
		LastErrorAtMs:  lastErrorAt,
		EventTypes:     append([]string(nil), p.eventTypes...),
		DeadLetterMode: a.cfg.events.deadLetterMode,
		OperationId:    snap.operationID,
		EventStore:     cutUTF8Bytes(a.cfg.eventStore.Name(), maxStatusNameBytes),
		Capabilities:   capabilityNames(a.cfg.eventStore.Capabilities()),
	}

	if !snap.started {
		return status
	}
	// The subscription's slice may be its own; sort a copy so the status never reorders state
	// under the add-on.
	segments := append([]SegmentStatus(nil), p.run.segments()...)
	sort.SliceStable(segments, func(i, j int) bool {
		return SegmentOrder(segments[i].Segment, segments[j].Segment) < 0
	})
	replaying := false
	for _, segment := range segments {
		if len(segment.Segment) > MaxSegmentBytes {
			continue // the hub drops a longer name
		}
		replayUntil := a.replayUntil(p, segment, snap.replay)
		if replayUntil != "" {
			replaying = true
		}
		lag := segment.Lag
		if lag < 0 {
			lag = -1
		}
		status.Segments = append(status.Segments, &hubpb.SegmentStatus{
			Segment:     segment.Segment,
			Position:    boundedPosition(segment.Position),
			Committed:   boundedPosition(segment.Committed),
			Head:        boundedPosition(segment.Head),
			Lag:         lag,
			ReplayUntil: boundedPosition(replayUntil),
		})
	}
	status.Replaying = replaying
	return status
}

// capabilityNames is the store's capabilities as the status lists them: distinct, sorted, each
// within the protocol's name bound, at most maxStatusCapabilities.
func capabilityNames(capabilities []Capability) []string {
	names := make([]string, 0, len(capabilities))
	seen := map[string]struct{}{}
	for _, capability := range capabilities {
		name := cutUTF8Bytes(capability.String(), maxStatusNameBytes)
		if name == "" {
			continue
		}
		if _, twice := seen[name]; twice {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) > maxStatusCapabilities {
		names = names[:maxStatusCapabilities]
	}
	return names
}

// replayUntil is the end of the open replay range of segment that this instance's subscription
// has not passed yet: the largest Until among the ranges whose progress (the committed position,
// else the next one) is still before it. "" when the segment is not replaying.
func (a *App) replayUntil(p *processor, segment SegmentStatus, ranges []ReplayRange) string {
	progress := segment.Committed
	if progress == "" {
		progress = segment.Position
	}
	until := ""
	for _, replay := range ranges {
		if replay.Source != p.subscription.Source || replay.Segment != segment.Segment ||
			replay.Until == "" {
			continue
		}
		if progress != "" && a.cfg.eventStore.Compare(segment.Segment, progress, replay.Until) >= 0 {
			continue
		}
		if until == "" || a.cfg.eventStore.Compare(segment.Segment, until, replay.Until) < 0 {
			until = replay.Until
		}
	}
	return until
}

// boundedPosition is a position as a status carries it: "" when unknown or longer than the
// protocol allows (never cut: a cut one is wrong).
func boundedPosition(position string) string {
	if len(position) > MaxPositionBytes {
		return ""
	}
	return position
}

// cutUTF8Bytes cuts text to at most max bytes at a code point boundary.
func cutUTF8Bytes(text string, max int) string {
	if max <= 0 {
		return ""
	}
	if len(text) <= max {
		return text
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut]
}

// deadLetterSampler refreshes the processors' letter counts: every deadLetterSample, and when a
// park asked for an earlier look. It runs off the status tick and the consumer threads.
func (a *App) deadLetterSampler(ctx context.Context) error {
	interval := a.cfg.timings.deadLetterSample
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	// A processor that starts with letters parked by another instance reports them at once, as
	// the reference samples on its first status tick.
	a.sampleDeadLetterCounts(ctx, true)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			a.sampleDeadLetterCounts(ctx, true)
		case <-a.deadLetterKick:
			a.sampleDeadLetterCounts(ctx, false)
		}
	}
}

// sampleDeadLetterCounts refreshes every processor's count: all of them when force, else those
// that parked a letter since the last sample. Outside mode collection the count reads 0.
func (a *App) sampleDeadLetterCounts(ctx context.Context, force bool) {
	if a.cfg.store == nil {
		return
	}
	for _, p := range a.processors {
		if a.cfg.events.deadLetterMode != DeadLetterModeCollection {
			p.deadLetterCount.Store(0)
			continue
		}
		if !force && !p.parkedSinceSample.Swap(false) {
			continue
		}
		count, err := a.cfg.store.DeadLetters().Count(ctx, p.name)
		if err != nil {
			p.parkedSinceSample.Store(true)
			if ctx.Err() == nil {
				a.log.Debug("could not count a processor's dead letters",
					"processor", p.name, "error", err)
			}
			continue
		}
		if count < 0 {
			count = 0
		}
		p.deadLetterCount.Store(uint64(count))
	}
}

// kickDeadLetterSample asks the sampler to look before its next tick, without waiting for it.
func (a *App) kickDeadLetterSample() {
	if a.deadLetterKick == nil || a.cfg.store == nil ||
		a.cfg.events.deadLetterMode != DeadLetterModeCollection {
		return
	}
	select {
	case a.deadLetterKick <- struct{}{}:
	default:
	}
}
