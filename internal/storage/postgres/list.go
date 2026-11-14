package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
)

func (s *Store) ListStandaloneJobs(ctx context.Context, status domain.Status, page domain.PageParams) (domain.JobListPage, error) {
	var total int
	if status != "" {
		if err := s.pool.QueryRow(ctx, `
			SELECT count(*) FROM jobs WHERE campaign_id IS NULL AND status = $1`, string(status)).Scan(&total); err != nil {
			return domain.JobListPage{}, err
		}
	} else if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE campaign_id IS NULL`).Scan(&total); err != nil {
		return domain.JobListPage{}, err
	}

	var rows pgx.Rows
	var err error
	if status != "" {
		rows, err = s.pool.Query(ctx, `
			SELECT id::text, recipient_id, status, scheduled_at, created_at, updated_at
			FROM jobs
			WHERE campaign_id IS NULL AND status = $3
			ORDER BY created_at DESC, id DESC
			LIMIT $1 OFFSET $2`, page.Limit, page.Offset, string(status))
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id::text, recipient_id, status, scheduled_at, created_at, updated_at
			FROM jobs
			WHERE campaign_id IS NULL
			ORDER BY created_at DESC, id DESC
			LIMIT $1 OFFSET $2`, page.Limit, page.Offset)
	}
	if err != nil {
		return domain.JobListPage{}, err
	}
	defer rows.Close()
	items, err := scanJobListItems(rows)
	if err != nil {
		return domain.JobListPage{}, err
	}
	return domain.JobListPage{Items: items, Total: total, Limit: page.Limit, Offset: page.Offset}, nil
}

func scanJobListItems(rows pgx.Rows) ([]domain.JobListItem, error) {
	var items []domain.JobListItem
	for rows.Next() {
		var item domain.JobListItem
		var id, st string
		if err := rows.Scan(&id, &item.RecipientID, &st, &item.ScheduledAt, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		parsed, err := uuid.Parse(id)
		if err != nil {
			return nil, err
		}
		item.ID = parsed
		item.Status = domain.Status(st)
		items = append(items, item)
	}
	if items == nil {
		items = []domain.JobListItem{}
	}
	return items, rows.Err()
}

func (s *Store) ListCampaigns(ctx context.Context, page domain.PageParams) (domain.CampaignListPage, error) {
	var total int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM campaigns`).Scan(&total); err != nil {
		return domain.CampaignListPage{}, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id::text, scheduled_at, sender, created_at
		FROM campaigns
		ORDER BY created_at DESC, id DESC
		LIMIT $1 OFFSET $2`, page.Limit, page.Offset)
	if err != nil {
		return domain.CampaignListPage{}, err
	}
	defer rows.Close()

	var summaries []domain.CampaignSummary
	var campaignIDs []uuid.UUID
	for rows.Next() {
		var id, sender string
		var summary domain.CampaignSummary
		if err := rows.Scan(&id, &summary.ScheduledAt, &sender, &summary.CreatedAt); err != nil {
			return domain.CampaignListPage{}, err
		}
		parsed, err := uuid.Parse(id)
		if err != nil {
			return domain.CampaignListPage{}, err
		}
		summary.ID = parsed
		summary.Sender = sender
		summary.Counts = allStatusCounts()
		summaries = append(summaries, summary)
		campaignIDs = append(campaignIDs, parsed)
	}
	if err := rows.Err(); err != nil {
		return domain.CampaignListPage{}, err
	}
	if len(summaries) == 0 {
		return domain.CampaignListPage{Items: []domain.CampaignSummary{}, Total: total, Limit: page.Limit, Offset: page.Offset}, nil
	}

	countRows, err := s.pool.Query(ctx, `
		SELECT campaign_id::text, status, count(*)::int
		FROM jobs
		WHERE campaign_id = ANY($1::uuid[])
		GROUP BY campaign_id, status`, campaignIDs)
	if err != nil {
		return domain.CampaignListPage{}, err
	}
	defer countRows.Close()

	totals := map[uuid.UUID]int{}
	for countRows.Next() {
		var cid, status string
		var n int
		if err := countRows.Scan(&cid, &status, &n); err != nil {
			return domain.CampaignListPage{}, err
		}
		id, err := uuid.Parse(cid)
		if err != nil {
			return domain.CampaignListPage{}, err
		}
		for i := range summaries {
			if summaries[i].ID == id {
				summaries[i].Counts[domain.Status(status)] = n
				totals[id] += n
				break
			}
		}
	}
	for i := range summaries {
		summaries[i].TotalJobs = totals[summaries[i].ID]
	}
	return domain.CampaignListPage{Items: summaries, Total: total, Limit: page.Limit, Offset: page.Offset}, nil
}

func (s *Store) ListCampaignJobs(ctx context.Context, campaignID uuid.UUID, status domain.Status, page domain.PageParams) (domain.CampaignJobListPage, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT true FROM campaigns WHERE id = $1::uuid`, campaignID.String()).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.CampaignJobListPage{}, domain.ErrCampaignNotFound
		}
		return domain.CampaignJobListPage{}, err
	}

	var total int
	if status != "" {
		if err := s.pool.QueryRow(ctx, `
			SELECT count(*) FROM jobs WHERE campaign_id = $1::uuid AND status = $2`,
			campaignID.String(), string(status)).Scan(&total); err != nil {
			return domain.CampaignJobListPage{}, err
		}
	} else if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE campaign_id = $1::uuid`, campaignID.String()).Scan(&total); err != nil {
		return domain.CampaignJobListPage{}, err
	}

	var rows pgx.Rows
	var err error
	if status != "" {
		rows, err = s.pool.Query(ctx, `
			SELECT id::text, recipient_id, status, scheduled_at, last_error, canceled_at, finished_at
			FROM jobs
			WHERE campaign_id = $1::uuid AND status = $4
			ORDER BY recipient_id ASC, id ASC
			LIMIT $2 OFFSET $3`, campaignID.String(), page.Limit, page.Offset, string(status))
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id::text, recipient_id, status, scheduled_at, last_error, canceled_at, finished_at
			FROM jobs
			WHERE campaign_id = $1::uuid
			ORDER BY recipient_id ASC, id ASC
			LIMIT $2 OFFSET $3`, campaignID.String(), page.Limit, page.Offset)
	}
	if err != nil {
		return domain.CampaignJobListPage{}, err
	}
	defer rows.Close()
	var items []domain.CampaignJobListItem
	for rows.Next() {
		var item domain.CampaignJobListItem
		var id, st string
		if err := rows.Scan(&id, &item.RecipientID, &st, &item.ScheduledAt, &item.LastError, &item.CanceledAt, &item.FinishedAt); err != nil {
			return domain.CampaignJobListPage{}, err
		}
		parsed, err := uuid.Parse(id)
		if err != nil {
			return domain.CampaignJobListPage{}, err
		}
		item.JobID = parsed
		item.Status = domain.Status(st)
		items = append(items, item)
	}
	if items == nil {
		items = []domain.CampaignJobListItem{}
	}
	return domain.CampaignJobListPage{Items: items, Total: total, Limit: page.Limit, Offset: page.Offset}, rows.Err()
}
