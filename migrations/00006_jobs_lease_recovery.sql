-- +goose Up
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_until timestamptz;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS claim_count int NOT NULL DEFAULT 0;

ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('scheduled', 'queued', 'processing', 'succeeded', 'failed', 'canceled', 'unknown'));

-- Existing processing rows without a lease are immediately eligible for recovery.
UPDATE jobs
SET lease_until = clock_timestamp() - interval '1 second'
WHERE status = 'processing' AND lease_until IS NULL;

CREATE INDEX IF NOT EXISTS jobs_recovery_lease_idx
    ON jobs (lease_until, id)
    WHERE status = 'processing';

-- +goose Down
DROP INDEX IF EXISTS jobs_recovery_lease_idx;

ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('scheduled', 'queued', 'processing', 'succeeded', 'failed', 'canceled'));

ALTER TABLE jobs DROP COLUMN IF EXISTS claim_count;
ALTER TABLE jobs DROP COLUMN IF EXISTS lease_until;