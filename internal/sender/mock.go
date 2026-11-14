package sender

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

type Job struct {
	ID            uuid.UUID
	RecipientID   string
	Payload       json.RawMessage
	ScheduledAt   time.Time
	SendStartedAt time.Time
}

type Sender interface {
	Send(ctx context.Context, job Job) error
}

// ConfigurableMock simulates send latency and probabilistic failures.
// It does not deliver email or SMS. Successful Send means the mock accepted the call only.
type ConfigurableMock struct {
	WorkerID    string
	Logger      *slog.Logger
	Delay       time.Duration
	FailureRate float64
	Rand        FloatSource
}

func (m ConfigurableMock) Send(ctx context.Context, job Job) error {
	if err := waitWithContext(ctx, m.Delay); err != nil {
		return err
	}
	if m.FailureRate > 0 {
		r := m.rng().Float64()
		if r < m.FailureRate {
			return ErrSimulatedSend
		}
	}
	m.Logger.Info("mock sender accepted",
		"job_id", job.ID.String(),
		"worker_id", m.WorkerID,
		"recipient_id", job.RecipientID,
	)
	return nil
}

func (m ConfigurableMock) rng() FloatSource {
	if m.Rand != nil {
		return m.Rand
	}
	return defaultRand{}
}

func waitWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Mock wraps ConfigurableMock with zero delay and no failures for tests and legacy call sites.
type Mock struct {
	WorkerID string
	Logger   *slog.Logger
}

func (m Mock) Send(ctx context.Context, job Job) error {
	return ConfigurableMock{WorkerID: m.WorkerID, Logger: m.Logger}.Send(ctx, job)
}
