package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
)

func (s *Store) CreateCampaign(ctx context.Context, in domain.NewCampaign) (domain.CampaignSummary, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.CampaignSummary{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var id string
	var scheduledAt time.Time
	var sender string
	err = tx.QueryRow(ctx, `
		INSERT INTO campaigns (idempotency_key, content_hash, scheduled_at, sender, template_subject, template_body)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id::text, scheduled_at, sender`,
		in.IdempotencyKey, in.ContentHash, in.ScheduledAt, in.Sender, in.TemplateSubject, in.TemplateBody,
	).Scan(&id, &scheduledAt, &sender)
	if err == nil {
		campaignID, err := uuid.Parse(id)
		if err != nil {
			return domain.CampaignSummary{}, false, err
		}
		batch := &pgx.Batch{}
		for _, rec := range in.Recipients {
			key := domain.CampaignJobIdempotencyKey(campaignID, rec.RecipientID)
			hash, err := domain.ContentHash(rec.RecipientID, in.Sender, in.ScheduledAt, rec.Payload)
			if err != nil {
				return domain.CampaignSummary{}, false, err
			}
			batch.Queue(`
				INSERT INTO jobs (idempotency_key, content_hash, campaign_id, recipient_id, scheduled_at, sender, payload, status)
				VALUES ($1, $2, $3::uuid, $4, $5, $6, $7::json, 'scheduled')`,
				key, hash, id, rec.RecipientID, in.ScheduledAt, in.Sender, string(rec.Payload),
			)
		}
		br := tx.SendBatch(ctx, batch)
		for range in.Recipients {
			if _, err := br.Exec(); err != nil {
				_ = br.Close()
				return domain.CampaignSummary{}, false, err
			}
		}
		if err := br.Close(); err != nil {
			return domain.CampaignSummary{}, false, err
		}
		summary, err := loadCampaignSummary(ctx, tx, campaignID)
		if err != nil {
			return domain.CampaignSummary{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.CampaignSummary{}, false, err
		}
		return summary, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.CampaignSummary{}, false, err
	}

	existing, hash, err := campaignByKey(ctx, tx, in.IdempotencyKey)
	if err != nil {
		return domain.CampaignSummary{}, false, err
	}
	if !bytes.Equal(hash, in.ContentHash) {
		return domain.CampaignSummary{}, false, domain.ErrConflict
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.CampaignSummary{}, false, err
	}
	return existing, false, nil
}

func campaignByKey(ctx context.Context, q pgx.Tx, key string) (domain.CampaignSummary, []byte, error) {
	row := q.QueryRow(ctx, `SELECT id::text, content_hash FROM campaigns WHERE idempotency_key = $1`, key)
	var id string
	var hash []byte
	if err := row.Scan(&id, &hash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.CampaignSummary{}, nil, domain.ErrCampaignNotFound
		}
		return domain.CampaignSummary{}, nil, err
	}
	campaignID, err := uuid.Parse(id)
	if err != nil {
		return domain.CampaignSummary{}, nil, err
	}
	summary, err := loadCampaignSummary(ctx, q, campaignID)
	if err != nil {
		return domain.CampaignSummary{}, nil, err
	}
	return summary, hash, nil
}

func (s *Store) GetCampaign(ctx context.Context, id uuid.UUID) (domain.CampaignSummary, error) {
	return loadCampaignSummary(ctx, s.pool, id)
}

func loadCampaignSummary(ctx context.Context, q interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}, id uuid.UUID) (domain.CampaignSummary, error) {
	rows, err := q.Query(ctx, `
		SELECT c.id::text, c.scheduled_at, c.sender, j.status, count(j.id)::int
		FROM campaigns c
		LEFT JOIN jobs j ON j.campaign_id = c.id
		WHERE c.id = $1::uuid
		GROUP BY c.id, c.scheduled_at, c.sender, j.status`,
		id.String(),
	)
	if err != nil {
		return domain.CampaignSummary{}, err
	}
	defer rows.Close()

	var campaignID uuid.UUID
	var scheduledAt time.Time
	var sender string
	counts := allStatusCounts()
	total := 0
	foundCampaign := false
	for rows.Next() {
		var cid, status string
		var n int
		if err := rows.Scan(&cid, &scheduledAt, &sender, &status, &n); err != nil {
			return domain.CampaignSummary{}, err
		}
		foundCampaign = true
		parsed, err := uuid.Parse(cid)
		if err != nil {
			return domain.CampaignSummary{}, err
		}
		campaignID = parsed
		if status == "" {
			continue
		}
		counts[domain.Status(status)] = n
		total += n
	}
	if err := rows.Err(); err != nil {
		return domain.CampaignSummary{}, err
	}
	if !foundCampaign {
		return domain.CampaignSummary{}, domain.ErrCampaignNotFound
	}
	return domain.CampaignSummary{
		ID:          campaignID,
		ScheduledAt: scheduledAt,
		Sender:      sender,
		TotalJobs:   total,
		Counts:      counts,
	}, nil
}

func allStatusCounts() map[domain.Status]int {
	return map[domain.Status]int{
		domain.StatusScheduled:  0,
		domain.StatusQueued:     0,
		domain.StatusProcessing: 0,
		domain.StatusSucceeded:  0,
		domain.StatusFailed:     0,
		domain.StatusCanceled:   0,
	}
}

func (s *Store) CancelCampaignRecipients(ctx context.Context, campaignID uuid.UUID, recipientIDs []string) ([]domain.RecipientCancelOutcome, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var exists bool
	if err := tx.QueryRow(ctx, `SELECT true FROM campaigns WHERE id = $1::uuid`, campaignID.String()).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrCampaignNotFound
		}
		return nil, err
	}

	type item struct {
		idx int
		id  string
	}
	order := make([]item, len(recipientIDs))
	for i, id := range recipientIDs {
		order[i] = item{idx: i, id: id}
	}
	slices.SortFunc(order, func(a, b item) int {
		return stringsCompare(a.id, b.id)
	})

	outcomes := make([]domain.RecipientCancelOutcome, len(recipientIDs))
	for _, item := range order {
		var jobID string
		err := tx.QueryRow(ctx, `
			SELECT id::text FROM jobs
			WHERE campaign_id = $1::uuid AND recipient_id = $2
			FOR UPDATE`,
			campaignID.String(), item.id,
		).Scan(&jobID)
		if errors.Is(err, pgx.ErrNoRows) {
			outcomes[item.idx] = domain.RecipientCancelOutcome{
				RecipientID: item.id,
				Result:      domain.CampaignCancelResultNotFound,
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		parsed, err := uuid.Parse(jobID)
		if err != nil {
			return nil, err
		}
		_, result, err := cancelJobTx(ctx, tx, parsed)
		if err != nil {
			if errors.Is(err, domain.ErrTooLate) {
				outcomes[item.idx] = domain.RecipientCancelOutcome{
					RecipientID: item.id,
					Result:      domain.CampaignCancelResultTooLate,
				}
				continue
			}
			return nil, err
		}
		switch result {
		case domain.CancelOutcomeCanceled:
			outcomes[item.idx] = domain.RecipientCancelOutcome{RecipientID: item.id, Result: domain.CampaignCancelResultCanceled}
		case domain.CancelOutcomeAlreadyCanceled:
			outcomes[item.idx] = domain.RecipientCancelOutcome{RecipientID: item.id, Result: domain.CampaignCancelResultAlreadyCanceled}
		default:
			return nil, fmt.Errorf("unexpected cancel outcome %q", result)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return outcomes, nil
}

func stringsCompare(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
