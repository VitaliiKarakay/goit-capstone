-- +goose Up
ALTER TABLE jobs DROP CONSTRAINT jobs_idempotency_key_key;

ALTER TABLE jobs ADD COLUMN campaign_id uuid;

CREATE TABLE campaigns (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key text NOT NULL,
    content_hash bytea NOT NULL,
    scheduled_at timestamptz NOT NULL,
    sender text NOT NULL,
    template_subject text NOT NULL,
    template_body text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT campaigns_idempotency_key_key UNIQUE (idempotency_key)
);

ALTER TABLE jobs
    ADD CONSTRAINT jobs_campaign_id_fkey FOREIGN KEY (campaign_id) REFERENCES campaigns (id);

CREATE UNIQUE INDEX jobs_standalone_idempotency_key_idx ON jobs (idempotency_key) WHERE campaign_id IS NULL;
CREATE UNIQUE INDEX jobs_campaign_recipient_idx ON jobs (campaign_id, recipient_id) WHERE campaign_id IS NOT NULL;
CREATE INDEX jobs_campaign_id_idx ON jobs (campaign_id) WHERE campaign_id IS NOT NULL;

-- +goose Down
DROP INDEX jobs_campaign_id_idx;
DROP INDEX jobs_campaign_recipient_idx;
DROP INDEX jobs_standalone_idempotency_key_idx;

ALTER TABLE jobs DROP CONSTRAINT jobs_campaign_id_fkey;
ALTER TABLE jobs DROP COLUMN campaign_id;

DROP TABLE campaigns;

ALTER TABLE jobs ADD CONSTRAINT jobs_idempotency_key_key UNIQUE (idempotency_key);
