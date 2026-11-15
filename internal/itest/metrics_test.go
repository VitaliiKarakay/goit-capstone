//go:build integration

package itest

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMetricsSnapshotScheduledCoherent(t *testing.T) {
	ctx := context.Background()
	counts, err := env.direct.MetricsSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sch := counts.ByStatus["scheduled"]
	if sch != counts.ScheduledFuture+counts.ScheduledOverdue {
		t.Fatalf("scheduled=%d future=%d overdue=%d", sch, counts.ScheduledFuture, counts.ScheduledOverdue)
	}
}

// Uses only future scheduled and terminal rows so the in-test scheduler cannot queue inserts.
func TestMetricsSnapshotExactDeltas(t *testing.T) {
	ctx := context.Background()
	before, err := env.direct.MetricsSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(24 * time.Hour).UTC()
	hash := []byte{0x01, 0x02, 0x03}

	insert := func(status string) {
		id := uuid.New()
		key := "met-delta-" + uuid.NewString()
		_, err := env.db.Exec(ctx, `
			INSERT INTO jobs (id, idempotency_key, content_hash, recipient_id, scheduled_at, sender, payload, status)
			VALUES ($1, $2, $3, $4, $5, 'mock', '{}', $6)`,
			id, key, hash, "r-"+uuid.NewString()[:8], future, status,
		)
		if err != nil {
			t.Fatal(err)
		}
	}

	insert("scheduled")
	insert("scheduled")
	insert("succeeded")
	insert("failed")
	insert("canceled")

	after, err := env.direct.MetricsSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}

	delta := func(status string) int {
		return after.ByStatus[status] - before.ByStatus[status]
	}
	if delta("scheduled") != 2 {
		t.Fatalf("scheduled delta=%d", delta("scheduled"))
	}
	if after.ScheduledFuture-before.ScheduledFuture != 2 {
		t.Fatalf("future delta=%d", after.ScheduledFuture-before.ScheduledFuture)
	}
	if after.ScheduledOverdue-before.ScheduledOverdue != 0 {
		t.Fatalf("overdue delta=%d", after.ScheduledOverdue-before.ScheduledOverdue)
	}
	if delta("succeeded") != 1 || delta("failed") != 1 || delta("canceled") != 1 {
		t.Fatalf("terminal deltas succeeded=%d failed=%d canceled=%d", delta("succeeded"), delta("failed"), delta("canceled"))
	}
	if delta("queued") != 0 {
		t.Fatalf("queued delta=%d", delta("queued"))
	}
	if after.OutboxUnpublished-before.OutboxUnpublished != 0 {
		t.Fatalf("outbox delta=%d", after.OutboxUnpublished-before.OutboxUnpublished)
	}
	if after.ByStatus["scheduled"] != after.ScheduledFuture+after.ScheduledOverdue {
		t.Fatal("scheduled must equal future+overdue after single-query snapshot")
	}
}
