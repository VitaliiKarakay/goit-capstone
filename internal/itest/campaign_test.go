//go:build integration

package itest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
)

func TestCampaignCreatesPersonalizedJobs(t *testing.T) {
	key := "camp-" + uuid.NewString()
	at := time.Now().Add(30 * time.Minute).UTC()
	body := campaignBody(key, at, []recipientSpec{
		{"user-a", "a@example.com", map[string]string{"name": "Alice", "sum": "10", "currency": "UAH"}},
		{"user-b", "b@example.com", map[string]string{"name": "Bob", "sum": "20", "currency": "UAH"}},
	})
	code, resp, err := env.postCampaign(body)
	if err != nil || code != http.StatusCreated {
		t.Fatalf("create %d err=%v resp=%v", code, err, resp)
	}
	campaignID, _ := resp["campaign_id"].(string)
	if int(resp["total_jobs"].(float64)) != 2 {
		t.Fatalf("total_jobs=%v", resp["total_jobs"])
	}
	var rows int
	if err := env.db.QueryRow(context.Background(), `SELECT count(*) FROM jobs WHERE campaign_id = $1::uuid`, campaignID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("rows=%d", rows)
	}
	var payload string
	if err := env.db.QueryRow(context.Background(), `
		SELECT payload::text FROM jobs WHERE campaign_id = $1::uuid AND recipient_id = 'user-a'`,
		campaignID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var p map[string]string
	_ = json.Unmarshal([]byte(payload), &p)
	if p["subject"] != "Alice, offer" || p["body"] != "10 UAH" {
		t.Fatalf("payload=%v", p)
	}
}

func TestCampaignValidationLeavesNoRows(t *testing.T) {
	beforeCamp, beforeJobs := tableCounts(t)
	key := "camp-bad-" + uuid.NewString()
	at := time.Now().Add(time.Hour).UTC()
	body := campaignBody(key, at, []recipientSpec{
		{"user-a", "a@example.com", campaignVars("A", "1", "UAH")},
		{"user-a", "b@example.com", campaignVars("B", "2", "UAH")},
	})
	code, _, err := env.postCampaign(body)
	if err == nil && code != http.StatusBadRequest {
		t.Fatalf("status=%d", code)
	}
	afterCamp, afterJobs := tableCounts(t)
	if afterCamp != beforeCamp || afterJobs != beforeJobs {
		t.Fatalf("counts changed camp %d->%d jobs %d->%d", beforeCamp, afterCamp, beforeJobs, afterJobs)
	}
}

func TestCampaignValidationMissingVariables(t *testing.T) {
	beforeCamp, beforeJobs := tableCounts(t)
	key := "camp-miss-" + uuid.NewString()
	at := time.Now().Add(time.Hour).UTC()
	body := campaignBody(key, at, []recipientSpec{
		{"user-a", "a@example.com", map[string]string{"name": "Only"}},
	})
	code, _, err := env.postCampaign(body)
	if err == nil && code != http.StatusBadRequest {
		t.Fatalf("status=%d", code)
	}
	afterCamp, afterJobs := tableCounts(t)
	if afterCamp != beforeCamp || afterJobs != beforeJobs {
		t.Fatalf("counts changed camp %d->%d jobs %d->%d", beforeCamp, afterCamp, beforeJobs, afterJobs)
	}
}

func TestCampaignCreateRollbackOnJobInsertFailure(t *testing.T) {
	ctx := context.Background()
	_, err := env.db.Exec(ctx, `
		CREATE OR REPLACE FUNCTION itest_fail_campaign_job() RETURNS trigger AS $$
		BEGIN
		  IF NEW.recipient_id = 'trigger-fail' THEN
		    RAISE EXCEPTION 'itest job insert fail';
		  END IF;
		  RETURN NEW;
		END;
		$$ LANGUAGE plpgsql`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = env.db.Exec(context.Background(), `DROP FUNCTION IF EXISTS itest_fail_campaign_job() CASCADE`)
	})
	_, err = env.db.Exec(ctx, `
		CREATE TRIGGER itest_fail_campaign_job_tr
		BEFORE INSERT ON jobs FOR EACH ROW EXECUTE FUNCTION itest_fail_campaign_job()`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = env.db.Exec(context.Background(), `DROP TRIGGER IF EXISTS itest_fail_campaign_job_tr ON jobs`)
	})

	key := "camp-roll-" + uuid.NewString()
	at := time.Now().Add(time.Hour).UTC()
	body := campaignBody(key, at, []recipientSpec{
		{"ok-user", "ok@example.com", campaignVars("OK", "1", "UAH")},
		{"trigger-fail", "x@example.com", campaignVars("Fail", "2", "UAH")},
	})
	code, _, err := env.postCampaign(body)
	if err == nil && code < 500 {
		t.Fatalf("expected failure got %d err=%v", code, err)
	}
	var campRows int
	if err := env.db.QueryRow(ctx, `SELECT count(*) FROM campaigns WHERE idempotency_key = $1`, key).Scan(&campRows); err != nil {
		t.Fatal(err)
	}
	if campRows != 0 {
		t.Fatalf("campaign rows=%d", campRows)
	}
	var jobRows int
	if err := env.db.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE recipient_id IN ('ok-user','trigger-fail') AND campaign_id IS NOT NULL`).Scan(&jobRows); err != nil {
		t.Fatal(err)
	}
	if jobRows != 0 {
		t.Fatalf("job rows=%d", jobRows)
	}
}

func TestCampaignConcurrentIdempotency(t *testing.T) {
	key := "camp-idem-" + uuid.NewString()
	at := time.Now().Add(time.Hour).UTC()
	body := campaignBody(key, at, []recipientSpec{
		{"user-1", "1@example.com", campaignVars("One", "10", "UAH")},
		{"user-2", "2@example.com", campaignVars("Two", "20", "UAH")},
	})
	const n = 12
	var wg sync.WaitGroup
	codes := make([]int, n)
	ids := make([]string, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			code, resp, err := env.postCampaign(body)
			if err != nil {
				t.Error(err)
				return
			}
			codes[i] = code
			ids[i], _ = resp["campaign_id"].(string)
		}(i)
	}
	wg.Wait()
	created := 0
	for i, code := range codes {
		if ids[i] == "" || ids[i] != ids[0] {
			t.Fatalf("id mismatch %s %s code=%d", ids[i], ids[0], code)
		}
		if code == http.StatusCreated {
			created++
		} else if code != http.StatusOK {
			t.Fatalf("code=%d", code)
		}
	}
	if created != 1 {
		t.Fatalf("created=%d", created)
	}
	var rows int
	if err := env.db.QueryRow(context.Background(), `SELECT count(*) FROM jobs WHERE campaign_id = $1::uuid`, ids[0]).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("rows=%d", rows)
	}
}

func TestCampaignIdempotencyPermutationAndConflict(t *testing.T) {
	key := "camp-hash-" + uuid.NewString()
	at := time.Now().Add(time.Hour).UTC()
	r1 := []recipientSpec{
		{"user-b", "b@x.com", campaignVars("B", "1", "UAH")},
		{"user-a", "a@x.com", campaignVars("A", "2", "UAH")},
	}
	code, resp, err := env.postCampaign(campaignBody(key, at, r1))
	if err != nil || code != http.StatusCreated {
		t.Fatal(err, code)
	}
	id := resp["campaign_id"].(string)
	r2 := []recipientSpec{
		{"user-a", "a@x.com", campaignVars("A", "2", "UAH")},
		{"user-b", "b@x.com", campaignVars("B", "1", "UAH")},
	}
	code, resp2, err := env.postCampaign(campaignBody(key, at, r2))
	if err != nil || code != http.StatusOK || resp2["campaign_id"].(string) != id {
		t.Fatalf("replay %d %v", code, resp2)
	}
	raw := campaignBody(key, at, r2)
	raw = strings.Replace(raw, "[name], offer", "[name], other", 1)
	code, _, err = env.postCampaign(raw)
	if err != nil || code != http.StatusConflict {
		t.Fatalf("conflict %d err=%v", code, err)
	}
}

func TestCampaignTargetedCancel(t *testing.T) {
	env.fault.pauseClaim.Store(true)
	t.Cleanup(func() { env.fault.pauseClaim.Store(false) })

	key := "camp-cancel-" + uuid.NewString()
	at := time.Now().Add(-time.Second).UTC()
	body := campaignBody(key, at, []recipientSpec{
		{"user-1", "1@x.com", campaignVars("One", "1", "UAH")},
		{"user-2", "2@x.com", campaignVars("Two", "2", "UAH")},
		{"user-3", "3@x.com", campaignVars("Three", "3", "UAH")},
	})
	code, resp, err := env.postCampaign(body)
	if err != nil || code != http.StatusCreated {
		t.Fatal(err)
	}
	campaignID := resp["campaign_id"].(string)
	env.waitCampaignQueued(t, campaignID, 3, 20*time.Second)

	cancelCode, cancelResp := env.cancelCampaign(campaignID, []string{"user-2", "user-1"})
	if cancelCode != http.StatusOK {
		t.Fatalf("cancel %d %v", cancelCode, cancelResp)
	}
	results, _ := cancelResp["results"].([]any)
	if len(results) != 2 {
		t.Fatalf("results=%v", cancelResp)
	}
	env.fault.pauseClaim.Store(false)
	env.waitCampaignSummary(t, campaignID, 1, 2, 60*time.Second)
	if env.rec.countByCampaign(t, campaignID) != 1 {
		t.Fatalf("sender calls=%d", env.rec.countByCampaign(t, campaignID))
	}
}

func TestCampaignCancelMixedAndReplay(t *testing.T) {
	key := "camp-mix-" + uuid.NewString()
	at := time.Now().Add(time.Hour).UTC()
	body := campaignBody(key, at, []recipientSpec{
		{"user-1", "1@x.com", campaignVars("One", "1", "UAH")},
	})
	code, resp, err := env.postCampaign(body)
	if err != nil || code != http.StatusCreated {
		t.Fatal(err)
	}
	campaignID := resp["campaign_id"].(string)
	var jobID string
	if err := env.db.QueryRow(context.Background(), `SELECT id::text FROM jobs WHERE campaign_id = $1::uuid AND recipient_id = 'user-1'`, campaignID).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	cancelCode, cancelResp := env.cancelCampaign(campaignID, []string{"user-1", "missing"})
	if cancelCode != http.StatusOK {
		t.Fatal(cancelResp)
	}
	_, cancelResp = env.cancelCampaign(campaignID, []string{"user-1"})
	checkResult(t, cancelResp, "user-1", "already_canceled")
	code, resp, err = env.postCampaign(body)
	if err != nil || code != http.StatusOK {
		t.Fatal(err)
	}
	summary := env.getCampaign(t, campaignID)
	if summary["counts"].(map[string]any)["canceled"].(float64) != 1 {
		t.Fatalf("summary=%v", summary)
	}
	_ = jobID
}

func TestCampaignSummaryMatchesJobs(t *testing.T) {
	key := "camp-sum-" + uuid.NewString()
	at := time.Now().Add(time.Hour).UTC()
	body := campaignBody(key, at, []recipientSpec{
		{"u1", "1@x.com", campaignVars("1", "10", "UAH")},
		{"u2", "2@x.com", campaignVars("2", "20", "UAH")},
	})
	_, resp, _ := env.postCampaign(body)
	id := resp["campaign_id"].(string)
	summary := env.getCampaign(t, id)
	counts := summary["counts"].(map[string]any)
	total := 0
	for _, k := range []string{"scheduled", "queued", "processing", "succeeded", "failed", "canceled"} {
		total += int(counts[k].(float64))
	}
	if total != int(summary["total_jobs"].(float64)) {
		t.Fatalf("sum mismatch total=%v counts=%v", summary["total_jobs"], counts)
	}
}

func TestStandaloneIdempotencyKeySpaceSeparate(t *testing.T) {
	at := time.Now().Add(time.Hour).UTC()
	userKey := "campaign:" + uuid.NewString() + ":recipient:human"
	jobBody := jobBody(userKey, "solo-user", at, "solo")
	if _, _, err := env.post(jobBody); err != nil {
		t.Fatal(err)
	}
	campKey := "camp-space-" + uuid.NewString()
	camp := campaignBody(campKey, at, []recipientSpec{
		{"user-c", "c@x.com", campaignVars("C", "1", "UAH")},
	})
	if _, _, err := env.postCampaign(camp); err != nil {
		t.Fatal(err)
	}
	var solo int
	if err := env.db.QueryRow(context.Background(), `SELECT count(*) FROM jobs WHERE idempotency_key = $1 AND campaign_id IS NULL`, userKey).Scan(&solo); err != nil {
		t.Fatal(err)
	}
	if solo != 1 {
		t.Fatalf("solo=%d", solo)
	}
}

func TestSingleJobStillWorksAfterCampaign(t *testing.T) {
	body := jobBody("after-camp-"+uuid.NewString(), "solo", time.Now().Add(-time.Second), "ok")
	env.waitStatus(t, mustPost(t, body), "succeeded", 30*time.Second)
}

type recipientSpec struct {
	id        string
	to        string
	variables map[string]string
}

func campaignVars(name, sum, currency string) map[string]string {
	return map[string]string{"name": name, "sum": sum, "currency": currency}
}

func campaignBody(key string, at time.Time, recipients []recipientSpec) string {
	rs := make([]any, len(recipients))
	for i, r := range recipients {
		rs[i] = map[string]any{
			"recipient_id": r.id,
			"to":           r.to,
			"variables":    r.variables,
		}
	}
	raw, _ := json.Marshal(map[string]any{
		"idempotency_key": key,
		"scheduled_at":    domain.FormatTime(at),
		"sender":          "mock",
		"template": map[string]string{
			"subject": "[name], offer",
			"body":    "[sum] [currency]",
		},
		"recipients": rs,
	})
	return string(raw)
}

func (e *environment) postCampaign(body string) (int, map[string]any, error) {
	res, err := http.Post(e.api+"/campaigns", "application/json", strings.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return res.StatusCode, nil, err
	}
	var payload map[string]any
	_ = json.Unmarshal(raw, &payload)
	if res.StatusCode != http.StatusCreated && res.StatusCode != http.StatusOK && res.StatusCode != http.StatusConflict && res.StatusCode != http.StatusBadRequest {
		return res.StatusCode, payload, fmt.Errorf("status %d body %s", res.StatusCode, raw)
	}
	return res.StatusCode, payload, nil
}

func (e *environment) getCampaign(t *testing.T, id string) map[string]any {
	t.Helper()
	res, err := http.Get(e.api + "/campaigns/" + id)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("get campaign %d %v", res.StatusCode, payload)
	}
	return payload
}

func (e *environment) cancelCampaign(id string, recipientIDs []string) (int, map[string]any) {
	raw, _ := json.Marshal(map[string]any{"recipient_ids": recipientIDs})
	res, err := http.Post(e.api+"/campaigns/"+id+"/cancel", "application/json", strings.NewReader(string(raw)))
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	var payload map[string]any
	_ = json.Unmarshal(body, &payload)
	return res.StatusCode, payload
}

func tableCounts(t *testing.T) (campaigns, jobs int) {
	t.Helper()
	if err := env.db.QueryRow(context.Background(), `SELECT count(*) FROM campaigns`).Scan(&campaigns); err != nil {
		t.Fatal(err)
	}
	if err := env.db.QueryRow(context.Background(), `SELECT count(*) FROM jobs`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	return campaigns, jobs
}

func (e *environment) waitCampaignQueued(t *testing.T, campaignID string, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var queued int
		_ = e.db.QueryRow(context.Background(), `
			SELECT count(*) FROM jobs WHERE campaign_id = $1::uuid AND status = 'queued'`, campaignID).Scan(&queued)
		if queued >= n {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("campaign %s not fully queued", campaignID)
}

func (e *environment) waitCampaignSummary(t *testing.T, campaignID string, wantSucceeded, wantCanceled int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s := env.getCampaign(t, campaignID)
		counts := s["counts"].(map[string]any)
		if int(counts["succeeded"].(float64)) == wantSucceeded && int(counts["canceled"].(float64)) == wantCanceled {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("summary timeout %+v", env.getCampaign(t, campaignID))
}

func (r *recorder) countByCampaign(t *testing.T, campaignID string) int {
	t.Helper()
	rows, err := env.db.Query(context.Background(), `SELECT id::text FROM jobs WHERE campaign_id = $1::uuid`, campaignID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	ids := map[uuid.UUID]struct{}{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		parsed := uuid.MustParse(id)
		ids[parsed] = struct{}{}
	}
	total := 0
	for id := range ids {
		total += r.count(id)
	}
	return total
}

func mustPost(t *testing.T, body string) string {
	t.Helper()
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func checkResult(t *testing.T, resp map[string]any, recipient, want string) {
	t.Helper()
	results, _ := resp["results"].([]any)
	for _, raw := range results {
		row, _ := raw.(map[string]any)
		if row["recipient_id"] == recipient && row["result"] == want {
			return
		}
	}
	t.Fatalf("result for %s not %s in %v", recipient, want, resp)
}
