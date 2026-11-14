package postgres

import (
	"context"

	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
	"github.com/VitaliiKarakay/goit-capstone/internal/metrics"
)

func (s *Store) MetricsSnapshot(ctx context.Context) (metrics.DBCounts, error) {
	var sch, que, proc, suc, fail, can, future, overdue, outbox int
	err := s.pool.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE status = 'scheduled')::int,
			count(*) FILTER (WHERE status = 'queued')::int,
			count(*) FILTER (WHERE status = 'processing')::int,
			count(*) FILTER (WHERE status = 'succeeded')::int,
			count(*) FILTER (WHERE status = 'failed')::int,
			count(*) FILTER (WHERE status = 'canceled')::int,
			count(*) FILTER (WHERE status = 'scheduled' AND scheduled_at > now())::int,
			count(*) FILTER (WHERE status = 'scheduled' AND scheduled_at <= now())::int,
			(SELECT count(*)::int FROM outbox WHERE published_at IS NULL)
		FROM jobs`).Scan(&sch, &que, &proc, &suc, &fail, &can, &future, &overdue, &outbox)
	if err != nil {
		return metrics.DBCounts{}, err
	}

	return metrics.DBCounts{
		ByStatus: map[string]int{
			string(domain.StatusScheduled):  sch,
			string(domain.StatusQueued):     que,
			string(domain.StatusProcessing): proc,
			string(domain.StatusSucceeded):  suc,
			string(domain.StatusFailed):     fail,
			string(domain.StatusCanceled):   can,
		},
		ScheduledFuture:   future,
		ScheduledOverdue:  overdue,
		OutboxUnpublished: outbox,
	}, nil
}
