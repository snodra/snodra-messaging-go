package sql

import (
	"context"
	"fmt"
	"time"
)

// Background runs the retention purge every PurgeInterval until ctx is done. A negative interval
// turns it off and it waits for ctx, as the lifecycle's background work lives as long as the app.
func (s *Store) Background(ctx context.Context) error {
	interval := s.settings.PurgeInterval
	if interval < 0 {
		<-ctx.Done()
		return nil
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			purgeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			_, _ = s.purgeExpired(purgeCtx)
			cancel()
		}
	}
}

// purgeExpired deletes what the retentions let go and the expired leases, in batches, each batch
// in a transaction of its own; how many rows went. Unsent outbox entries, live sagas, scheduled
// deadlines and fresh rows are never deleted.
func (s *Store) purgeExpired(ctx context.Context) (int64, error) {
	now := millis(s.now())
	before := func(retention time.Duration) time.Time { return now.Add(-retention) }
	total := int64(0)
	for _, step := range []struct {
		table     string
		key       string
		condition string
		bound     time.Time
	}{
		{"snodra_outbox", "id", "sent_at < ?", before(s.settings.OutboxRetention)},
		{"snodra_processed_events", "id_hash", "processed_at < ?", before(s.settings.ProcessedRetention)},
		{"snodra_external_calls", "id_hash", "updated_at < ?", before(s.settings.ExternalCallRetention)},
		{"snodra_sagas", "id", "ended = TRUE AND ended_at < ?", before(s.settings.SagaEndedRetention)},
		{"snodra_saga_dispatch", "id", "dispatched_at < ?", before(s.settings.SagaDispatchRetention)},
		{"snodra_saga_dispatch", "id", "failed_at < ?", before(s.settings.FailedRetention)},
		{"snodra_saga_dispatch_leases", "saga_id", "lease_until < ?", now},
		{"snodra_deadlines", "id", "finished_at < ?", before(s.settings.DeadlineRetention)},
		{"snodra_saga_failures", "id_hash", "last_failed_at < ?", before(s.settings.FailedRetention)},
		{"snodra_dead_letters", "id", "expire_at <= ?", now},
	} {
		purged, err := s.purgeTable(ctx, step.table, step.key, step.condition, step.bound)
		if err != nil {
			return total, err
		}
		total += purged
	}
	return total, nil
}

// purgeTable deletes the rows of table whose key matches condition (one parameter, before),
// purgeBatch at a time, each batch in a transaction of its own; how many.
func (s *Store) purgeTable(ctx context.Context, table, key, condition string, before time.Time) (int64, error) {
	var query string
	if s.driver == Postgres {
		query = fmt.Sprintf("DELETE FROM %s WHERE %s IN (SELECT %s FROM %s WHERE %s LIMIT %d)", table, key, key, table, condition, purgeBatch)
	} else {
		query = fmt.Sprintf("DELETE FROM %s WHERE %s LIMIT %d", table, condition, purgeBatch)
	}
	query = s.bind(query)
	total := int64(0)
	for {
		tx, err := s.beginOwn(ctx)
		if err != nil {
			return total, err
		}
		result, err := tx.ExecContext(ctx, query, before)
		if err != nil {
			_ = tx.Rollback()
			return total, storeError(err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			_ = tx.Rollback()
			return total, storeError(err)
		}
		if err := tx.Commit(); err != nil {
			return total, storeError(err)
		}
		total += affected
		if affected < purgeBatch {
			return total, nil
		}
	}
}
