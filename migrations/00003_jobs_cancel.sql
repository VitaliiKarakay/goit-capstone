-- +goose Up
ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('scheduled', 'queued', 'processing', 'succeeded', 'failed', 'canceled'));
ALTER TABLE jobs ADD COLUMN canceled_at timestamptz;

-- +goose Down
ALTER TABLE jobs DROP COLUMN canceled_at;
ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('scheduled', 'queued', 'processing', 'succeeded', 'failed'));
