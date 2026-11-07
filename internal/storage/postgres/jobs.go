package postgres

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
)

const jobColumns = `id::text, idempotency_key, campaign_id::text, recipient_id, scheduled_at, sender, payload::text, status, worker_id, last_error, send_started_at, send_attempt_id::text, canceled_at, created_at, updated_at, finished_at`

type OutboxMessage struct {
	ID    int64
	JobID uuid.UUID
}

func (s *Store) CreateJob(ctx context.Context, in domain.NewJob) (domain.Job, bool, error) {
	job, err := scanJob(s.pool.QueryRow(ctx, `
		INSERT INTO jobs (idempotency_key, content_hash, recipient_id, scheduled_at, sender, payload, status, campaign_id)
		VALUES ($1, $2, $3, $4, $5, $6::json, 'scheduled', NULL)
		ON CONFLICT (idempotency_key) WHERE campaign_id IS NULL DO NOTHING
		RETURNING `+jobColumns,
		in.IdempotencyKey, in.ContentHash, in.RecipientID, in.ScheduledAt, in.Sender, string(in.Payload),
	))
	if err == nil {
		return job, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, false, err
	}

	existing, hash, err := s.jobByKey(ctx, in.IdempotencyKey)
	if err != nil {
		return domain.Job{}, false, err
	}
	if !bytes.Equal(hash, in.ContentHash) {
		return domain.Job{}, false, domain.ErrConflict
	}
	return existing, false, nil
}

func (s *Store) GetJob(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	job, err := scanJob(s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1::uuid`, id.String()))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, domain.ErrNotFound
	}
	return job, err
}

// CancelJob transitions scheduled or queued jobs to canceled. It races with Claim on queued rows.
func (s *Store) CancelJob(ctx context.Context, id uuid.UUID) (domain.Job, domain.CancelOutcome, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Job{}, "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	job, outcome, err := cancelJobTx(ctx, tx, id)
	if err != nil {
		return domain.Job{}, "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Job{}, "", err
	}
	return job, outcome, nil
}

func cancelJobTx(ctx context.Context, tx pgx.Tx, id uuid.UUID) (domain.Job, domain.CancelOutcome, error) {
	job, err := scanJob(tx.QueryRow(ctx, `
		UPDATE jobs
		SET status = 'canceled', canceled_at = now(), updated_at = now()
		WHERE id = $1::uuid AND status IN ('scheduled', 'queued')
		RETURNING `+jobColumns, id.String()))
	if err == nil {
		return job, domain.CancelOutcomeCanceled, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, "", err
	}
	job, err = scanJob(tx.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1::uuid`, id.String()))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, "", domain.ErrNotFound
	}
	if err != nil {
		return domain.Job{}, "", err
	}
	switch job.Status {
	case domain.StatusCanceled:
		return job, domain.CancelOutcomeAlreadyCanceled, nil
	case domain.StatusProcessing, domain.StatusSucceeded, domain.StatusFailed:
		return domain.Job{}, "", domain.ErrTooLate
	case domain.StatusScheduled, domain.StatusQueued:
		job, err = scanJob(tx.QueryRow(ctx, `
			UPDATE jobs
			SET status = 'canceled', canceled_at = now(), updated_at = now()
			WHERE id = $1::uuid AND status IN ('scheduled', 'queued')
			RETURNING `+jobColumns, id.String()))
		if err == nil {
			return job, domain.CancelOutcomeCanceled, nil
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Job{}, "", domain.ErrTooLate
		}
		return domain.Job{}, "", err
	default:
		return domain.Job{}, "", domain.ErrTooLate
	}
}

func (s *Store) QueueDue(ctx context.Context, limit int) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		WITH due AS (
			SELECT id FROM jobs
			WHERE status = 'scheduled' AND scheduled_at <= now()
			ORDER BY scheduled_at, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs AS j
		SET status = 'queued', updated_at = now()
		FROM due
		WHERE j.id = due.id
		RETURNING j.id::text
	`, limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	rows.Close()

	for _, id := range ids {
		if _, err := tx.Exec(ctx, `INSERT INTO outbox (job_id) VALUES ($1::uuid)`, id); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(ids), nil
}

// PendingOutbox reads unpublished rows without holding a transaction.
// One publisher is assumed: a second process could publish the same row twice.
func (s *Store) PendingOutbox(ctx context.Context, limit int) ([]OutboxMessage, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, job_id::text
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var batch []OutboxMessage
	for rows.Next() {
		var msg OutboxMessage
		var jobID string
		if err := rows.Scan(&msg.ID, &jobID); err != nil {
			return nil, err
		}
		id, err := uuid.Parse(jobID)
		if err != nil {
			return nil, err
		}
		msg.JobID = id
		batch = append(batch, msg)
	}
	return batch, rows.Err()
}

func (s *Store) MarkPublished(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox SET published_at = now()
		WHERE id = $1 AND published_at IS NULL
	`, id)
	return err
}

// Claim reports whether the row existed and whether this call won the queued → processing update.
func (s *Store) Claim(ctx context.Context, id uuid.UUID, workerID string) (domain.Job, bool, bool, error) {
	job, err := scanJob(s.pool.QueryRow(ctx, `
		UPDATE jobs
		SET status = 'processing', worker_id = $2, send_attempt_id = gen_random_uuid(), updated_at = now()
		WHERE id = $1::uuid AND status = 'queued'
		RETURNING `+jobColumns, id.String(), workerID))
	if err == nil {
		return job, true, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, false, false, err
	}
	job, err = s.GetJob(ctx, id)
	if errors.Is(err, domain.ErrNotFound) {
		return domain.Job{}, false, false, nil
	}
	if err != nil {
		return domain.Job{}, false, false, err
	}
	return job, true, false, nil
}

func (s *Store) MarkSendStarted(ctx context.Context, id, attempt uuid.UUID) (time.Time, error) {
	var started time.Time
	err := s.pool.QueryRow(ctx, `
		UPDATE jobs
		SET send_started_at = now(), updated_at = now()
		WHERE id = $1::uuid AND status = 'processing' AND send_attempt_id = $2::uuid AND send_started_at IS NULL
		RETURNING send_started_at
	`, id.String(), attempt.String()).Scan(&started)
	if err == nil {
		return started, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, err
	}
	err = s.pool.QueryRow(ctx, `
		SELECT send_started_at
		FROM jobs
		WHERE id = $1::uuid AND status = 'processing' AND send_attempt_id = $2::uuid AND send_started_at IS NOT NULL
	`, id.String(), attempt.String()).Scan(&started)
	if err == nil {
		return started, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, domain.ErrNotProcessing
	}
	return time.Time{}, err
}

func (s *Store) Finish(ctx context.Context, id uuid.UUID, status domain.Status, lastError *string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = $2, last_error = $3, finished_at = now(), updated_at = now()
		WHERE id = $1::uuid AND status = 'processing'
	`, id.String(), string(status), lastError)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotProcessing
	}
	return nil
}

func (s *Store) jobByKey(ctx context.Context, key string) (domain.Job, []byte, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+jobColumns+`, content_hash FROM jobs WHERE idempotency_key = $1 AND campaign_id IS NULL`, key)
	var job domain.Job
	var id, payload, status string
	var attempt *string
	var hash []byte
	var campaignID *string
	err := row.Scan(
		&id, &job.IdempotencyKey, &campaignID, &job.RecipientID, &job.ScheduledAt, &job.Sender, &payload, &status,
		&job.WorkerID, &job.LastError, &job.SendStartedAt, &attempt, &job.CanceledAt, &job.CreatedAt, &job.UpdatedAt, &job.FinishedAt, &hash,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, nil, domain.ErrNotFound
	}
	if err != nil {
		return domain.Job{}, nil, err
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return domain.Job{}, nil, err
	}
	job.ID = parsed
	job.Payload = []byte(payload)
	job.Status = domain.Status(status)
	if err := applyCampaignID(campaignID, &job); err != nil {
		return domain.Job{}, nil, err
	}
	if err := scanAttempt(attempt, &job); err != nil {
		return domain.Job{}, nil, err
	}
	return job, hash, nil
}

func scanJob(row pgx.Row) (domain.Job, error) {
	var job domain.Job
	var id, payload, status string
	var campaignID *string
	var attempt *string
	err := row.Scan(
		&id, &job.IdempotencyKey, &campaignID, &job.RecipientID, &job.ScheduledAt, &job.Sender, &payload, &status,
		&job.WorkerID, &job.LastError, &job.SendStartedAt, &attempt, &job.CanceledAt, &job.CreatedAt, &job.UpdatedAt, &job.FinishedAt,
	)
	if err != nil {
		return domain.Job{}, err
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return domain.Job{}, err
	}
	job.ID = parsed
	job.Payload = []byte(payload)
	job.Status = domain.Status(status)
	if err := applyCampaignID(campaignID, &job); err != nil {
		return domain.Job{}, err
	}
	if err := scanAttempt(attempt, &job); err != nil {
		return domain.Job{}, err
	}
	return job, nil
}

func applyCampaignID(raw *string, job *domain.Job) error {
	if raw == nil || *raw == "" {
		return nil
	}
	parsed, err := uuid.Parse(*raw)
	if err != nil {
		return err
	}
	job.CampaignID = &parsed
	return nil
}

func scanAttempt(raw *string, job *domain.Job) error {
	if raw == nil || *raw == "" {
		return nil
	}
	parsed, err := uuid.Parse(*raw)
	if err != nil {
		return err
	}
	job.SendAttemptID = &parsed
	return nil
}
