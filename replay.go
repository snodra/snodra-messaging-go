package snodra

// DisallowReplay keeps an event handler from running again when a reset replays the record: the
// handler runs for a live delivery, and not inside an open replay range. The stage-1 marker is
// still recorded and the record is still acknowledged, so the replay moves on. Every handler of
// the type but the ones marked this way runs; with every handler marked, the record is handled
// as a duplicate would be. A dead letter parked during a replay is retried as a replay too, so
// this handler stays out of that retry.
//
// It is an [EventOption] of [HandleEvent], like [InProcessor]:
//
//	snodra.HandleEvent[OrderPlaced](app, sendMail, snodra.InProcessor("mail"), snodra.DisallowReplay())
func DisallowReplay() EventOption {
	return func(c *eventConfig) { c.disallowReplay = true }
}

// inReplay reports whether d is inside an open replay range of its processor's source. Positions
// are ordered by the event store only.
func (a *App) inReplay(p *processor, d Delivery) bool {
	for _, replay := range p.run.replayRanges() {
		if replay.Source != p.subscription.Source || replay.Segment != d.Segment || replay.Until == "" {
			continue
		}
		if a.cfg.eventStore.Compare(d.Segment, replay.From, d.Position) <= 0 &&
			a.cfg.eventStore.Compare(d.Segment, d.Position, replay.Until) < 0 {
			return true
		}
	}
	return false
}
