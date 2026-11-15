//go:build integration

package itest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
	"github.com/VitaliiKarakay/goit-capstone/internal/worker"
)

func seedExpiredProcessing(t *testing.T, recipient, body string, sendStarted bool, claimCount int) (string, uuid.UUID) {
	t.Helper()
	env.fault.pauseClaim.Store(true)
	defer env.fault.pauseClaim.Store(false)

	at := time.Now().UTC().Add(-time.Minute)
	key := recipient + "-" + uuid.NewString()
	postBody := jobBody(key, recipient, at, body)
	_, id, err := env.post(postBody)
	if err != nil {
		t.Fatal(err)
	}
	env.waitStatus(t, id, "queued", 15*time.Second)
	jobID := uuid.MustParse(id)
	attempt := uuid.New()
	var q string
	var args []any
	if sendStarted {
		q = `
		UPDATE jobs
		SET status = 'processing',
		    worker_id = 'ghost',
		    send_attempt_id = $2,
		    send_started_at = clock_timestamp() - interval '2 minutes',
		    lease_until = clock_timestamp() - interval '1 minute',
		    claim_count = $3
		WHERE id = $1`
		args = []any{jobID, attempt, claimCount}
	} else {
		q = `
		UPDATE jobs
		SET status = 'processing',
		    worker_id = 'ghost',
		    send_attempt_id = $2,
		    lease_until = clock_timestamp() - interval '1 minute',
		    claim_count = $3
		WHERE id = $1 AND status = 'queued'`
		args = []any{jobID, attempt, claimCount}
	}
	if _, err := env.db.Exec(context.Background(), q, args...); err != nil {
		t.Fatal(err)
	}
	return id, jobID
}

func outboxRows(t *testing.T, jobID uuid.UUID) (total, pending int) {
	t.Helper()
	if err := env.db.QueryRow(context.Background(), `
		SELECT count(*)::int,
		       count(*) FILTER (WHERE published_at IS NULL)::int
		FROM outbox WHERE job_id = $1::uuid`, jobID).Scan(&total, &pending); err != nil {
		t.Fatal(err)
	}
	return total, pending
}

func TestRecoveryRequeuesExpiredLeaseWithoutSend(t *testing.T) {
	ctx := context.Background()
	id, jobID := seedExpiredProcessing(t, "recovery-no-send", "body", false, 1)

	totalBefore, _ := outboxRows(t, jobID)
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
	totalAfter, pendingAfter := outboxRows(t, jobID)
	if totalAfter-totalBefore != 1 {
		t.Fatalf("outbox delta=%d want 1", totalAfter-totalBefore)
	}
	if pendingAfter < 1 {
		t.Fatalf("pending outbox=%d", pendingAfter)
	}

	env.waitStatus(t, id, "succeeded", 30*time.Second)
	if env.rec.count(jobID) != 1 {
		t.Fatalf("send calls=%d want 1", env.rec.count(jobID))
	}
}

func TestRecoveryUnknownAfterSendStarted(t *testing.T) {
	ctx := context.Background()
	id, _ := seedExpiredProcessing(t, "recovery-unknown", "body", true, 1)

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
	max := worker.DefaultMaxClaimAttempts
	id, _ := seedExpiredProcessing(t, "recovery-maxed", "body", false, max)

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

func TestConcurrentRecoveryCreatesSingleOutboxRow(t *testing.T) {
	ctx := context.Background()
	_, jobID := seedExpiredProcessing(t, "recovery-race", "body", false, 1)
	totalBefore, _ := outboxRows(t, jobID)

	var wg sync.WaitGroup
	var aStats, bStats domain.RecoveryStats
	var aErr, bErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		aStats, aErr = env.direct.RecoverExpiredLeases(ctx, 10, worker.DefaultMaxClaimAttempts)
	}()
	go func() {
		defer wg.Done()
		bStats, bErr = env.direct.RecoverExpiredLeases(ctx, 10, worker.DefaultMaxClaimAttempts)
	}()
	wg.Wait()
	if aErr != nil || bErr != nil {
		t.Fatalf("errors: %v %v", aErr, bErr)
	}
	if aStats.Requeued+bStats.Requeued != 1 {
		t.Fatalf("requeued total=%d", aStats.Requeued+bStats.Requeued)
	}
	totalAfter, _ := outboxRows(t, jobID)
	if totalAfter-totalBefore != 1 {
		t.Fatalf("outbox delta=%d want 1", totalAfter-totalBefore)
	}
}

func TestLongProcessingRenewsLeaseDuringMarkDelay(t *testing.T) {
	env.fault.markSleep.Store(int64(2 * time.Second))
	defer env.fault.markSleep.Store(0)
	env.runnerBy["worker-a"].SetLeasePolicy(700*time.Millisecond, 120*time.Millisecond, 30*time.Second)

	at := time.Now().UTC().Add(-time.Minute)
	body := jobBody("renew-mark-"+uuid.NewString(), "renew-user", at, "body")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	jobID := uuid.MustParse(id)
	env.waitStatus(t, id, "succeeded", 30*time.Second)
	if env.rec.count(jobID) != 1 {
		t.Fatalf("send calls=%d", env.rec.count(jobID))
	}
}

func TestStaleFinishRejectedAfterRecoveryClaim(t *testing.T) {
	ctx := context.Background()
	id, jobID := seedExpiredProcessing(t, "stale-finish", "body", false, 1)
	var oldAttempt uuid.UUID
	if err := env.db.QueryRow(ctx, `SELECT send_attempt_id FROM jobs WHERE id=$1`, jobID).Scan(&oldAttempt); err != nil {
		t.Fatal(err)
	}
	if _, err := env.direct.RecoverExpiredLeases(ctx, 10, worker.DefaultMaxClaimAttempts); err != nil {
		t.Fatal(err)
	}
	env.waitStatus(t, id, "succeeded", 30*time.Second)
	text := "stale"
	err := env.direct.FinishAttempt(ctx, jobID, oldAttempt, "ghost", domain.StatusSucceeded, &text)
	if err == nil {
		t.Fatal("expected stale finish to fail")
	}
	got := env.get(t, id)
	if got["status"] != string(domain.StatusSucceeded) {
		t.Fatalf("status=%v", got["status"])
	}
}

func TestUnknownRedeliveryDoesNotSend(t *testing.T) {
	ctx := context.Background()
	id, jobID := seedExpiredProcessing(t, "unknown-redeliver", "body", true, 1)
	if _, err := env.direct.RecoverExpiredLeases(ctx, 10, worker.DefaultMaxClaimAttempts); err != nil {
		t.Fatal(err)
	}
	env.waitStatus(t, id, "unknown", 5*time.Second)
	before := env.rec.count(jobID)
	if _, err := env.db.Exec(ctx, `INSERT INTO outbox (job_id) VALUES ($1::uuid)`, jobID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if env.rec.count(jobID) > before {
			t.Fatalf("send count %d -> %d", before, env.rec.count(jobID))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestCampaignSummaryIncludesUnknown(t *testing.T) {
	at := time.Now().Add(time.Minute).UTC()
	body := campaignBody("unknown-camp-"+uuid.NewString(), at, []recipientSpec{
		{id: "u-a", to: "a@ex.com", variables: campaignVars("A", "1", "USD")},
		{id: "u-b", to: "b@ex.com", variables: campaignVars("B", "2", "USD")},
	})
	code, resp, err := env.postCampaign(body)
	if err != nil || code != 201 {
		t.Fatalf("create campaign %d %v", code, err)
	}
	cid := resp["campaign_id"].(string)
	var jobID uuid.UUID
	if err := env.db.QueryRow(context.Background(), `SELECT id FROM jobs WHERE campaign_id=$1::uuid LIMIT 1`, cid).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := env.db.Exec(context.Background(), `
		UPDATE jobs SET status='unknown', last_error=$2, finished_at=clock_timestamp(), worker_id=NULL, lease_until=NULL
		WHERE id=$1`, jobID, domain.LastErrorLeaseExpiredAfterSendStarted); err != nil {
		t.Fatal(err)
	}
	summary := env.getCampaign(t, cid)
	counts, _ := summary["counts"].(map[string]any)
	if int(counts["unknown"].(float64)) != 1 {
		t.Fatalf("counts=%v", counts)
	}
}

func TestCancelAllCountsUnknownAsTooLate(t *testing.T) {
	at := time.Now().Add(time.Minute).UTC()
	body := campaignBody("unknown-cancel-"+uuid.NewString(), at, []recipientSpec{
		{id: "u-c", to: "c@ex.com", variables: campaignVars("C", "1", "USD")},
	})
	code, resp, err := env.postCampaign(body)
	if err != nil || code != 201 {
		t.Fatal(err)
	}
	cid := resp["campaign_id"].(string)
	var jobID uuid.UUID
	if err := env.db.QueryRow(context.Background(), `SELECT id FROM jobs WHERE campaign_id=$1::uuid LIMIT 1`, cid).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := env.db.Exec(context.Background(), `
		UPDATE jobs SET status='unknown', last_error=$2, finished_at=clock_timestamp()
		WHERE id=$1`, jobID, domain.LastErrorLeaseExpiredAfterSendStarted); err != nil {
		t.Fatal(err)
	}
	code, out := env.cancelCampaignAll(cid)
	if code != 200 {
		t.Fatalf("cancel-all %d %v", code, out)
	}
	if int(out["too_late"].(float64)) != 1 {
		t.Fatalf("resp=%v", out)
	}
}
