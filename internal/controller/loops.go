package controller

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/VitaliiKarakay/goit-capstone/internal/storage/postgres"
)

type dueQueue interface {
	QueueDue(ctx context.Context, limit int) (int, error)
}

type outboxStore interface {
	PendingOutbox(ctx context.Context, limit int) ([]postgres.OutboxMessage, error)
	MarkPublished(ctx context.Context, id int64) error
}

type jobPublisher interface {
	PublishJob(ctx context.Context, jobID uuid.UUID, messageID string) error
}

func RunScheduler(ctx context.Context, log *slog.Logger, store dueQueue, interval time.Duration, batch int) {
	runLoop(ctx, interval, func() {
		n, err := store.QueueDue(ctx, batch)
		if err != nil {
			log.Error("scheduler", "err", err)
			return
		}
		if n > 0 {
			log.Info("queued due jobs", "count", n)
		}
	})
}

// RunOutbox publishes each pending row and marks it afterwards.
// The database transaction is not held across the NATS call.
// A single publisher is assumed; two publishers could emit the same job twice.
func RunOutbox(ctx context.Context, log *slog.Logger, store outboxStore, bus jobPublisher, interval time.Duration, batch int) {
	runLoop(ctx, interval, func() {
		pending, err := store.PendingOutbox(ctx, batch)
		if err != nil {
			log.Error("outbox read", "err", err)
			return
		}
		for _, msg := range pending {
			if ctx.Err() != nil {
				return
			}
			messageID := strconv.FormatInt(msg.ID, 10)
			if err := bus.PublishJob(ctx, msg.JobID, messageID); err != nil {
				log.Error("outbox publish", "outbox_id", msg.ID, "job_id", msg.JobID.String(), "err", err)
				return
			}
			if err := store.MarkPublished(ctx, msg.ID); err != nil {
				log.Error("outbox mark", "outbox_id", msg.ID, "job_id", msg.JobID.String(), "err", err)
				return
			}
			log.Info("outbox published", "outbox_id", msg.ID, "job_id", msg.JobID.String())
		}
	})
}

func runLoop(ctx context.Context, interval time.Duration, tick func()) {
	tick()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick()
		}
	}
}
