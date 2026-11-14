-- +goose Up
ALTER TABLE jobs ADD COLUMN send_attempt_id uuid;

-- +goose Down
ALTER TABLE jobs DROP COLUMN send_attempt_id;
