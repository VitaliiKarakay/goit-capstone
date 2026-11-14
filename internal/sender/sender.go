package sender

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/google/uuid"
)

type Job struct {
	ID          uuid.UUID
	RecipientID string
	Payload     json.RawMessage
}

type Sender interface {
	Send(ctx context.Context, job Job) error
}

// Mock accepts every message and records the call in the process log.
// It does not deliver email or SMS. A sender error is not produced here;
// the worker still records a terminal failure when Send returns an error.
type Mock struct {
	WorkerID string
	Logger   *slog.Logger
}

func (m Mock) Send(_ context.Context, job Job) error {
	m.Logger.Info("mock sender accepted",
		"job_id", job.ID.String(),
		"worker_id", m.WorkerID,
		"recipient_id", job.RecipientID,
	)
	return nil
}
