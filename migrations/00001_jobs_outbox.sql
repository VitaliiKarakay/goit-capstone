-- +goose Up
CREATE TABLE jobs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key text NOT NULL,
    content_hash bytea NOT NULL,
    recipient_id text NOT NULL,
    scheduled_at timestamptz NOT NULL,
    sender text NOT NULL,
    payload json NOT NULL,
    status text NOT NULL,
    worker_id text,
    last_error text,
    send_started_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    CONSTRAINT jobs_idempotency_key_key UNIQUE (idempotency_key),
    CONSTRAINT jobs_status_check CHECK (status IN ('scheduled', 'queued', 'processing', 'succeeded', 'failed'))
);

CREATE INDEX jobs_scheduled_due_idx ON jobs (scheduled_at, id) WHERE status = 'scheduled';

CREATE TABLE outbox (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES jobs (id),
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

CREATE INDEX outbox_pending_idx ON outbox (id) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE outbox;
DROP TABLE jobs;
