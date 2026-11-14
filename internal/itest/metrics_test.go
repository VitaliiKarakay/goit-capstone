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

func TestMetricsSnapshotExactDeltas(t *testing.T) {
	ctx := context.Background()
	before, err := env.direct.MetricsSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tag := uuid.NewString()[:8]
	future := time.Now().Add(2 * time.Hour).UTC()
	past := time.Now().Add(-2 * time.Hour).UTC()
	hash := []byte{0x01, 0x02, 0x03}

	insert := func(status string, at time.Time, outbox bool) {
		id := uuid.New()
		key := "met-delta-" + tag + "-" + uuid.NewString()
		_, err := env.db.Exec(ctx, `
			INSERT INTO jobs (id, idempotency_key, content_hash, recipient_id, scheduled_at, sender, payload, status)
			VALUES ($1, $2, $3, $4, $5, 'mock', '{}', $6)`,
			id, key, hash, "r-"+uuid.NewString()[:8], at, status,
		)
		if err != nil {
			t.Fatal(err)
		}
		if outbox {
			if _, err := env.db.Exec(ctx, `INSERT INTO outbox (job_id) VALUES ($1)`, id); err != nil {
				t.Fatal(err)
			}
		}
	}

	insert("scheduled", future, false)
	insert("scheduled", past, false)
	insert("queued", past, true)
	insert("succeeded", past, false)
	insert("failed", past, false)

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
	if after.ScheduledFuture-before.ScheduledFuture != 1 {
		t.Fatalf("future delta=%d", after.ScheduledFuture-before.ScheduledFuture)
	}
	if after.ScheduledOverdue-before.ScheduledOverdue != 1 {
		t.Fatalf("overdue delta=%d", after.ScheduledOverdue-before.ScheduledOverdue)
	}
	if delta("queued") != 1 {
		t.Fatalf("queued delta=%d", delta("queued"))
	}
	if delta("succeeded") != 1 {
		t.Fatalf("succeeded delta=%d", delta("succeeded"))
	}
	if delta("failed") != 1 {
		t.Fatalf("failed delta=%d", delta("failed"))
	}
	if after.OutboxUnpublished-before.OutboxUnpublished != 1 {
		t.Fatalf("outbox delta=%d", after.OutboxUnpublished-before.OutboxUnpublished)
	}
	if after.ByStatus["scheduled"] != after.ScheduledFuture+after.ScheduledOverdue {
		t.Fatal("scheduled must equal future+overdue after single-query snapshot")
	}
}
