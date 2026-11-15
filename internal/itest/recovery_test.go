//go:build integration

package itest

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
	"github.com/VitaliiKarakay/goit-capstone/internal/worker"
)

func TestRecoveryRequeuesExpiredLeaseWithoutSend(t *testing.T) {
	ctx := context.Background()
	env.fault.pauseClaim.Store(true)
	defer env.fault.pauseClaim.Store(false)

	at := time.Now().UTC().Add(-time.Minute)
	body := jobBody("recv-"+uuid.NewString(), "recovery-no-send", at, "body")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	env.waitStatus(t, id, "queued", 15*time.Second)

	jobID := uuid.MustParse(id)
	attempt := uuid.New()
	_, err = env.db.Exec(ctx, `
		UPDATE jobs
		SET status = 'processing',
		    worker_id = 'ghost',
		    send_attempt_id = $2,
		    lease_until = clock_timestamp() - interval '1 minute',
		    claim_count = 1
		WHERE id = $1 AND status = 'queued'`, jobID, attempt)
	if err != nil {
		t.Fatal(err)
	}

	stats, err := env.direct.RecoverExpiredLeases(ctx, 10, worker.DefaultMaxClaimAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Requeued != 1 {
		t.Fatalf("requeued=%d want 1", stats.Requeued)
	}
	got := env.get(t, id)
	if got["status"] != string(domain.StatusQueued) {
		t.Fatalf("status=%v", got["status"])
	}
	var pending int
	if err := env.db.QueryRow(ctx, `
		SELECT count(*)::int FROM outbox WHERE job_id = $1::uuid AND published_at IS NULL`, jobID).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending < 1 {
		t.Fatalf("unpublished outbox rows=%d want >=1", pending)
	}
}

func TestRecoveryUnknownAfterSendStarted(t *testing.T) {
	ctx := context.Background()
	env.fault.pauseClaim.Store(true)
	defer env.fault.pauseClaim.Store(false)

	at := time.Now().UTC().Add(-time.Minute)
	body := jobBody("recv-"+uuid.NewString(), "recovery-unknown", at, "body")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	env.waitStatus(t, id, "queued", 15*time.Second)

	jobID := uuid.MustParse(id)
	attempt := uuid.New()
	_, err = env.db.Exec(ctx, `
		UPDATE jobs
		SET status = 'processing',
		    worker_id = 'ghost',
		    send_attempt_id = $2,
		    send_started_at = clock_timestamp() - interval '2 minutes',
		    lease_until = clock_timestamp() - interval '1 minute',
		    claim_count = 1
		WHERE id = $1`, jobID, attempt)
	if err != nil {
		t.Fatal(err)
	}

	stats, err := env.direct.RecoverExpiredLeases(ctx, 10, worker.DefaultMaxClaimAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Unknown != 1 {
		t.Fatalf("unknown=%d want 1", stats.Unknown)
	}
	got := env.get(t, id)
	if got["status"] != string(domain.StatusUnknown) {
		t.Fatalf("status=%v", got["status"])
	}
	if got["last_error"] != domain.LastErrorLeaseExpiredAfterSendStarted {
		t.Fatalf("last_error=%v", got["last_error"])
	}
}

func TestRecoveryClaimExhaustedBeforeSend(t *testing.T) {
	ctx := context.Background()
	env.fault.pauseClaim.Store(true)
	defer env.fault.pauseClaim.Store(false)

	at := time.Now().UTC().Add(-time.Minute)
	body := jobBody("recv-"+uuid.NewString(), "recovery-maxed", at, "body")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	env.waitStatus(t, id, "queued", 15*time.Second)

	jobID := uuid.MustParse(id)
	max := worker.DefaultMaxClaimAttempts
	_, err = env.db.Exec(ctx, `
		UPDATE jobs
		SET status = 'processing',
		    worker_id = 'ghost',
		    send_attempt_id = gen_random_uuid(),
		    lease_until = clock_timestamp() - interval '1 minute',
		    claim_count = $2
		WHERE id = $1`, jobID, max)
	if err != nil {
		t.Fatal(err)
	}

	stats, err := env.direct.RecoverExpiredLeases(ctx, 10, max)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ClaimExhausted != 1 {
		t.Fatalf("claim_exhausted=%d want 1", stats.ClaimExhausted)
	}
	got := env.get(t, id)
	if got["status"] != string(domain.StatusFailed) {
		t.Fatalf("status=%v", got["status"])
	}
	if got["last_error"] != domain.LastErrorMaxClaimAttempts {
		t.Fatalf("last_error=%v", got["last_error"])
	}
}
