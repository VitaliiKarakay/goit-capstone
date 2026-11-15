package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
)

func (s *Store) RecoverExpiredLeases(ctx context.Context, batch, maxClaimAttempts int) (domain.RecoveryStats, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.RecoveryStats{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id::text, send_started_at IS NOT NULL AS send_started, claim_count
		FROM jobs
		WHERE status = 'processing' AND lease_until < clock_timestamp()
		ORDER BY lease_until, id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, batch)
	if err != nil {
		return domain.RecoveryStats{}, err
	}
	defer rows.Close()

	type row struct {
		id          string
		sendStarted bool
		claimCount  int
	}
	var items []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.sendStarted, &r.claimCount); err != nil {
			return domain.RecoveryStats{}, err
		}
		items = append(items, r)
	}
	if err := rows.Err(); err != nil {
		return domain.RecoveryStats{}, err
	}
	rows.Close()

	var stats domain.RecoveryStats
	for _, item := range items {
		switch {
		case !item.sendStarted:
			if item.claimCount >= maxClaimAttempts {
				tag, err := tx.Exec(ctx, `
					UPDATE jobs
					SET status = 'failed',
					    last_error = $2,
					    finished_at = clock_timestamp(),
					    updated_at = clock_timestamp(),
					    lease_until = NULL
					WHERE id = $1::uuid AND status = 'processing' AND lease_until < clock_timestamp()`,
					item.id, "max_claim_attempts_exceeded")
				if err != nil {
					return domain.RecoveryStats{}, err
				}
				if tag.RowsAffected() > 0 {
					stats.ClaimExhausted++
				}
				continue
			}
			tag, err := tx.Exec(ctx, `
				UPDATE jobs
				SET status = 'queued',
				    worker_id = NULL,
				    send_attempt_id = NULL,
				    send_started_at = NULL,
				    lease_until = NULL,
				    updated_at = clock_timestamp()
				WHERE id = $1::uuid AND status = 'processing' AND lease_until < clock_timestamp()`,
				item.id)
			if err != nil {
				return domain.RecoveryStats{}, err
			}
			if tag.RowsAffected() == 0 {
				continue
			}
			if _, err := tx.Exec(ctx, `INSERT INTO outbox (job_id) VALUES ($1::uuid)`, item.id); err != nil {
				return domain.RecoveryStats{}, err
			}
			stats.Requeued++
		default:
			tag, err := tx.Exec(ctx, `
				UPDATE jobs
				SET status = 'unknown',
				    last_error = $2,
				    finished_at = clock_timestamp(),
				    updated_at = clock_timestamp(),
				    lease_until = NULL
				WHERE id = $1::uuid AND status = 'processing' AND lease_until < clock_timestamp()`,
				item.id, "lease_expired_after_send_started")
			if err != nil {
				return domain.RecoveryStats{}, err
			}
			if tag.RowsAffected() > 0 {
				stats.Unknown++
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.RecoveryStats{}, err
	}
	return stats, nil
}

// LeaseExpired reports whether a processing job's lease has expired (real-time clock).
func (s *Store) LeaseExpired(ctx context.Context, id string) (bool, error) {
	var expired bool
	err := s.pool.QueryRow(ctx, `
		SELECT lease_until IS NULL OR lease_until < clock_timestamp()
		FROM jobs WHERE id = $1::uuid AND status = 'processing'`, id).Scan(&expired)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	return expired, err
}
