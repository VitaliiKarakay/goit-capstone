-- +goose Up
CREATE INDEX IF NOT EXISTS jobs_standalone_list_idx
    ON jobs (created_at DESC, id DESC) WHERE campaign_id IS NULL;

CREATE INDEX IF NOT EXISTS jobs_standalone_status_list_idx
    ON jobs (status, created_at DESC, id DESC) WHERE campaign_id IS NULL;

CREATE INDEX IF NOT EXISTS jobs_campaign_recipient_list_idx
    ON jobs (campaign_id, recipient_id ASC, id ASC) WHERE campaign_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS campaigns_list_idx
    ON campaigns (created_at DESC, id DESC);

-- +goose Down
DROP INDEX IF EXISTS campaigns_list_idx;
DROP INDEX IF EXISTS jobs_campaign_recipient_list_idx;
DROP INDEX IF EXISTS jobs_standalone_status_list_idx;
DROP INDEX IF EXISTS jobs_standalone_list_idx;