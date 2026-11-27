package snodra

import (
	"context"
	"sort"
	"strings"
	"time"
)

// memoryDeadLetters is MemoryMessageStore's dead-letter store (mode collection).
type memoryDeadLetters struct {
	store *MemoryMessageStore
}

// Park inserts letter; ParkAlready when a letter of that id exists (a redelivery). It is a write
// of its own, also when the context carries a transaction.
func (m memoryDeadLetters) Park(ctx context.Context, letter DeadLetter) (ParkResult, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	m.store.initLocked()
	m.store.dropExpiredDeadLettersLocked()
	if _, exists := m.store.data.deadLetters.get(nil, letter.ID); exists {
		return ParkAlready, nil
	}
	copied := copyDeadLetter(letter)
	m.store.data.deadLetters.set(nil, &m.store.data, letter.ID, &copied)
	return ParkNew, nil
}

// Head is the id of the head letter of (processor, key): the earliest position among its letters
// that are not INVALID (then the lowest id); ok is false when there is none.
func (m memoryDeadLetters) Head(ctx context.Context, processor, key string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	m.store.initLocked()
	m.store.dropExpiredDeadLettersLocked()
	id, ok := m.firstLocked(processor, key, nil)
	return id, ok, nil
}

// Earlier is the id of the earliest letter of (processor, key) at a position before position that
// is not INVALID: what blocks a retry. ok is false when there is none.
func (m memoryDeadLetters) Earlier(ctx context.Context, processor, key, position string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	m.store.initLocked()
	m.store.dropExpiredDeadLettersLocked()
	before := memoryPositionOf(position)
	id, ok := m.firstLocked(processor, key, &before)
	return id, ok, nil
}

// firstLocked is the head of (processor, key), optionally only at a position before before.
func (m memoryDeadLetters) firstLocked(processor, key string, before *memoryPosition) (string, bool) {
	var (
		found    string
		foundPos memoryPosition
		ok       bool
	)
	for id, row := range m.store.data.deadLetters.rows {
		letter := row.value
		if letter.Processor != processor || letter.Key == nil || *letter.Key != key ||
			letter.Reason == DeadLetterInvalid {
			continue
		}
		position := memoryPositionOf(letter.Position)
		if before != nil && position.compare(*before) >= 0 {
			continue
		}
		if !ok || position.compare(foundPos) < 0 || (position.compare(foundPos) == 0 && id < found) {
			found, foundPos, ok = id, position, true
		}
	}
	return found, ok
}

// ParkedKeys is the keys with a letter (not INVALID) of processor on the given segments of
// source; each key once, in no particular order, none for no segments.
func (m memoryDeadLetters) ParkedKeys(ctx context.Context, processor, source string, segments []string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return nil, nil
	}
	wanted := make(map[string]struct{}, len(segments))
	for _, segment := range segments {
		wanted[segment] = struct{}{}
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	m.store.initLocked()
	m.store.dropExpiredDeadLettersLocked()
	seen := map[string]struct{}{}
	var keys []string
	for _, row := range m.store.data.deadLetters.rows {
		letter := row.value
		if letter.Processor != processor || letter.Source != source || letter.Key == nil ||
			letter.Reason == DeadLetterInvalid {
			continue
		}
		if _, on := wanted[letter.Segment]; !on {
			continue
		}
		if _, once := seen[*letter.Key]; once {
			continue
		}
		seen[*letter.Key] = struct{}{}
		keys = append(keys, *letter.Key)
	}
	sort.Strings(keys)
	return keys, nil
}

// Get returns the letter, or nil when there is none.
func (m memoryDeadLetters) Get(ctx context.Context, id string) (*DeadLetter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	m.store.initLocked()
	m.store.dropExpiredDeadLettersLocked()
	letter, ok := m.store.data.deadLetters.get(nil, id)
	if !ok {
		return nil, nil
	}
	copied := copyDeadLetter(*letter)
	return &copied, nil
}

// List returns letters by ParkedAt, then id, every processor's when processor is empty, at most
// limit when limit > 0.
func (m memoryDeadLetters) List(ctx context.Context, processor string, limit int) ([]DeadLetter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	m.store.initLocked()
	m.store.dropExpiredDeadLettersLocked()
	letters := make([]DeadLetter, 0, len(m.store.data.deadLetters.rows))
	for _, row := range m.store.data.deadLetters.rows {
		letter := row.value
		if processor != "" && letter.Processor != processor {
			continue
		}
		letters = append(letters, copyDeadLetter(letter))
	}
	sort.Slice(letters, func(i, j int) bool {
		if !letters[i].ParkedAt.Equal(letters[j].ParkedAt) {
			return letters[i].ParkedAt.Before(letters[j].ParkedAt)
		}
		return letters[i].ID < letters[j].ID
	})
	if limit > 0 && len(letters) > limit {
		letters = letters[:limit]
	}
	return letters, nil
}

// Summaries returns every letter of processor, without its record, by position as Head orders
// them, then id.
func (m memoryDeadLetters) Summaries(ctx context.Context, processor string) ([]DeadLetterSummary, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	m.store.initLocked()
	m.store.dropExpiredDeadLettersLocked()
	summaries := make([]DeadLetterSummary, 0, len(m.store.data.deadLetters.rows))
	for _, row := range m.store.data.deadLetters.rows {
		letter := row.value
		if letter.Processor != processor {
			continue
		}
		summaries = append(summaries, DeadLetterSummary{
			ID:       letter.ID,
			Key:      copyStringPointer(letter.Key),
			Position: letter.Position,
			Reason:   letter.Reason,
			ParkedAt: letter.ParkedAt,
		})
	}
	sort.Slice(summaries, func(i, j int) bool {
		a, b := memoryPositionOf(summaries[i].Position), memoryPositionOf(summaries[j].Position)
		if c := a.compare(b); c != 0 {
			return c < 0
		}
		return summaries[i].ID < summaries[j].ID
	})
	return summaries, nil
}

// Count is how many letters there are; every processor's when processor is empty.
func (m memoryDeadLetters) Count(ctx context.Context, processor string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	m.store.initLocked()
	m.store.dropExpiredDeadLettersLocked()
	var count int64
	for _, row := range m.store.data.deadLetters.rows {
		if processor == "" || row.value.Processor == processor {
			count++
		}
	}
	return count, nil
}

// Delete deletes the letter; false when there was none.
func (m memoryDeadLetters) Delete(ctx context.Context, id string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	m.store.initLocked()
	m.store.dropExpiredDeadLettersLocked()
	if _, exists := m.store.data.deadLetters.get(nil, id); !exists {
		return false, nil
	}
	m.store.data.deadLetters.set(nil, &m.store.data, id, nil)
	return true, nil
}

// DeleteIn deletes the letter in the transaction carried by ctx: a retry's, which commits with
// the record's marker and its handlers' effects or not at all. False when there was none.
func (m memoryDeadLetters) DeleteIn(ctx context.Context, id string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	deleted := false
	_, err := m.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[DeadLetter]
		if tx != nil {
			overlay = &tx.deadLetters
		}
		if _, exists := data.deadLetters.get(overlay, id); !exists {
			return nil
		}
		data.deadLetters.set(overlay, data, id, nil)
		deleted = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return deleted, nil
}

// RecordFailure records that a retry failed again: Attempts + 1, Error, LastAttemptAt = now,
// reason HANDLER_FAILED, no BlockedBy any more. False when the letter is gone.
func (m memoryDeadLetters) RecordFailure(ctx context.Context, id string, failure DeadLetterError, now time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	m.store.initLocked()
	m.store.dropExpiredDeadLettersLocked()
	letter, exists := m.store.data.deadLetters.get(nil, id)
	if !exists {
		return false, nil
	}
	copied := copyDeadLetter(*letter)
	copied.Attempts++
	copied.Error = NewDeadLetterError(failure.Type, failure.Message)
	copied.LastAttemptAt = now
	copied.Reason = DeadLetterHandlerFailed
	copied.BlockedBy = ""
	m.store.data.deadLetters.set(nil, &m.store.data, id, &copied)
	return true, nil
}

// dropExpiredDeadLettersLocked removes the letters whose retention passed, as a database's TTL
// would, before a call reads them. The store's clock is the application's.
func (s *MemoryMessageStore) dropExpiredDeadLettersLocked() {
	now := s.nowLocked()
	for id, row := range s.data.deadLetters.rows {
		if !row.value.ExpireAt.IsZero() && row.value.ExpireAt.Before(now) {
			delete(s.data.deadLetters.rows, id)
		}
	}
}

// copyDeadLetter copies everything a caller could change behind the store's back.
func copyDeadLetter(letter DeadLetter) DeadLetter {
	copied := letter
	copied.Key = copyStringPointer(letter.Key)
	copied.Headers = copyStringMap(letter.Headers)
	copied.Payload = append([]byte(nil), letter.Payload...)
	return copied
}

// compare orders positions as the document stores keep them: two numbers by value, two texts by
// bytes, and a number before a text (a database's mixed order).
func (p memoryPosition) compare(other memoryPosition) int {
	switch {
	case !p.isText && !other.isText:
		switch {
		case p.number < other.number:
			return -1
		case p.number > other.number:
			return 1
		default:
			return 0
		}
	case p.isText && other.isText:
		return strings.Compare(p.text, other.text)
	case p.isText:
		return 1
	default:
		return -1
	}
}
