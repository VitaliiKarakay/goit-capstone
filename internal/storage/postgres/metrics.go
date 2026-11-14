package postgres

import (
	"context"

	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
	"github.com/VitaliiKarakay/goit-capstone/internal/metrics"
)

func (s *Store) MetricsSnapshot(ctx context.Context) (metrics.DBCounts, error) {
	byStatus := map[string]int{
		string(domain.StatusScheduled):  0,
		string(domain.StatusQueued):     0,
		string(domain.StatusProcessing): 0,
		string(domain.StatusSucceeded):  0,
		string(domain.StatusFailed):     0,
		string(domain.StatusCanceled):   0,
	}
	rows, err := s.pool.Query(ctx, `SELECT status, count(*)::int FROM jobs GROUP BY status`)
	if err != nil {
		return metrics.DBCounts{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return metrics.DBCounts{}, err
		}
		byStatus[status] = n
	}
	if err := rows.Err(); err != nil {
		return metrics.DBCounts{}, err
	}

	var future, overdue, outbox int
	err = s.pool.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE status = 'scheduled' AND scheduled_at > now())::int,
			count(*) FILTER (WHERE status = 'scheduled' AND scheduled_at <= now())::int,
			(SELECT count(*)::int FROM outbox WHERE published_at IS NULL)
		FROM jobs`).Scan(&future, &overdue, &outbox)
	if err != nil {
		return metrics.DBCounts{}, err
	}

	return metrics.DBCounts{
		ByStatus:          byStatus,
		ScheduledFuture:   future,
		ScheduledOverdue:  overdue,
		OutboxUnpublished: outbox,
	}, nil
}
