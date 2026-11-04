//go:build integration

package itest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	natspub "github.com/VitaliiKarakay/goit-capstone/internal/messaging/nats"
)

func TestCancelFutureJob(t *testing.T) {
	scheduled := time.Now().Add(8 * time.Second).UTC().Truncate(time.Microsecond)
	body := jobBody("cancel-future-"+uuid.NewString(), "user-future", scheduled, "future")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	jobID := uuid.MustParse(id)

	code, resp := env.cancelJob(id)
	assertCancelAPI(t, code, resp, id, jobID)

	deadline := scheduled.Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job := env.get(t, id)
		if job["status"] != "canceled" {
			t.Fatalf("status after scheduled_at = %v", job["status"])
		}
		if env.rec.count(jobID) != 0 {
			t.Fatalf("sender calls after scheduled_at = %d", env.rec.count(jobID))
		}
		if time.Now().After(scheduled) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if time.Now().Before(scheduled) {
		t.Fatal("scheduled_at was not reached")
	}
}

func TestCancelQueuedWithPausedWorkers(t *testing.T) {
	env.fault.pauseClaim.Store(true)
	t.Cleanup(func() { env.fault.pauseClaim.Store(false) })

	body := jobBody("cancel-queued-"+uuid.NewString(), "user-queued", time.Now().Add(-time.Second), "queued")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	jobID := uuid.MustParse(id)
	env.waitJobStatus(t, id, "queued", 15*time.Second)

	code, resp := env.cancelJob(id)
	assertCancelAPI(t, code, resp, id, jobID)

	env.fault.pauseClaim.Store(false)
	time.Sleep(3 * time.Second)
	job := env.get(t, id)
	if job["status"] != "canceled" {
		t.Fatalf("status = %v", job["status"])
	}
	if env.rec.count(jobID) != 0 {
		t.Fatalf("sender calls = %d", env.rec.count(jobID))
	}
}

func TestCancelAlreadyCanceled(t *testing.T) {
	body := jobBody("cancel-twice-"+uuid.NewString(), "user-twice", time.Now().Add(time.Hour), "twice")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	jobID := uuid.MustParse(id)
	code, resp := env.cancelJob(id)
	assertCancelAPI(t, code, resp, id, jobID)
	code, resp = env.cancelJob(id)
	if code != http.StatusOK || resp["result"] != "already_canceled" {
		t.Fatalf("second cancel = %d %v", code, resp)
	}
	if env.get(t, id)["status"] != "canceled" || env.rec.count(jobID) != 0 {
		t.Fatal("canceled job was processed")
	}
}

func TestCancelAfterClaimTooLate(t *testing.T) {
	env.fault.markSleep.Store(int64(10 * time.Second))
	t.Cleanup(func() { env.fault.markSleep.Store(0) })

	body := jobBody("cancel-late-"+uuid.NewString(), "user-late", time.Now().Add(-time.Second), "late")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	env.waitJobStatus(t, id, "processing", 20*time.Second)
	code, resp := env.cancelJob(id)
	if code != http.StatusConflict {
		t.Fatalf("cancel = %d %v", code, resp)
	}
	errObj, _ := resp["error"].(map[string]any)
	if errObj["code"] != "too_late" {
		t.Fatalf("error = %v", resp)
	}
}

func TestCancelRacesClaimFromQueued(t *testing.T) {
	env.fault.pauseClaim.Store(true)
	t.Cleanup(func() { env.fault.pauseClaim.Store(false) })

	body := jobBody("cancel-race-"+uuid.NewString(), "user-race", time.Now().Add(-time.Second), "race")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	jobID := uuid.MustParse(id)
	env.waitJobStatus(t, id, "queued", 15*time.Second)

	release := make(chan struct{})
	var (
		cancelCode int
		cancelResp map[string]any
		wg         sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-release
		cancelCode, cancelResp = env.cancelJob(id)
	}()
	go func() {
		defer wg.Done()
		<-release
		env.fault.pauseClaim.Store(false)
	}()
	close(release)
	wg.Wait()
	time.Sleep(2 * time.Second)

	if cancelCode == http.StatusOK {
		result, _ := cancelResp["result"].(string)
		if result != "canceled" && result != "already_canceled" {
			t.Fatalf("unexpected cancel result = %v", cancelResp)
		}
		job := env.get(t, id)
		if job["status"] != "canceled" {
			t.Fatalf("status = %v after successful cancel", job["status"])
		}
		if env.rec.count(jobID) != 0 {
			t.Fatalf("sender calls = %d after successful cancel", env.rec.count(jobID))
		}
		return
	}
	if cancelCode != http.StatusConflict {
		t.Fatalf("cancel = %d %v", cancelCode, cancelResp)
	}
	env.waitStatus(t, id, "succeeded", 30*time.Second)
	if env.rec.count(jobID) != 1 {
		t.Fatalf("sender calls = %d after claim won", env.rec.count(jobID))
	}
}

func TestCanceledJobRedeliveryDoesNotSend(t *testing.T) {
	env.fault.pauseClaim.Store(true)
	t.Cleanup(func() { env.fault.pauseClaim.Store(false) })

	body := jobBody("cancel-redel-"+uuid.NewString(), "user-redel", time.Now().Add(-time.Second), "redel")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	jobID := uuid.MustParse(id)
	env.waitJobStatus(t, id, "queued", 15*time.Second)
	code, resp := env.cancelJob(id)
	assertCancelAPI(t, code, resp, id, jobID)

	env.fault.pauseClaim.Store(false)
	_, err = env.js.Publish(natspub.Subject, []byte(id), nats.MsgId("cancel-redel-"+id))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	if env.rec.count(jobID) != 0 {
		t.Fatalf("sender calls after redelivery = %d", env.rec.count(jobID))
	}
	if env.get(t, id)["status"] != "canceled" {
		t.Fatal("status changed after redelivery")
	}
}

func TestCreateAfterCancelReturnsSameJob(t *testing.T) {
	key := "cancel-replay-" + uuid.NewString()
	at := time.Now().Add(time.Hour).UTC()
	body := jobBody(key, "user-replay", at, "replay")
	code, id, err := env.post(body)
	if err != nil || code != http.StatusCreated {
		t.Fatalf("create = %d err=%v", code, err)
	}
	jobID := uuid.MustParse(id)
	cancelCode, cancelResp := env.cancelJob(id)
	assertCancelAPI(t, cancelCode, cancelResp, id, jobID)

	replayCode, replayID, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	if replayCode != http.StatusOK || replayID != id {
		t.Fatalf("replay = %d id=%s", replayCode, replayID)
	}
	job := env.get(t, id)
	if job["status"] != "canceled" {
		t.Fatalf("status = %v", job["status"])
	}
	var rows int
	if err := env.db.QueryRow(context.Background(), `SELECT count(*) FROM jobs WHERE idempotency_key = $1`, key).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("rows = %d", rows)
	}
	if env.rec.count(jobID) != 0 {
		t.Fatalf("sender calls = %d", env.rec.count(jobID))
	}
}

func assertCancelAPI(t *testing.T, code int, resp map[string]any, id string, jobID uuid.UUID) {
	t.Helper()
	if code != http.StatusOK {
		t.Fatalf("cancel %s = %d %v", id, code, resp)
	}
	result, _ := resp["result"].(string)
	if result != "canceled" && result != "already_canceled" {
		t.Fatalf("cancel result = %v", resp)
	}
	if resp["status"] != "canceled" {
		t.Fatalf("cancel status field = %v", resp["status"])
	}
	job := env.get(t, id)
	if job["status"] != "canceled" {
		t.Fatalf("job status = %v", job["status"])
	}
	if env.rec.count(jobID) != 0 {
		t.Fatalf("sender calls = %d", env.rec.count(jobID))
	}
}

func (e *environment) cancelJob(id string) (int, map[string]any) {
	req, err := http.NewRequest(http.MethodPost, e.api+"/jobs/"+id+"/cancel", http.NoBody)
	if err != nil {
		panic(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}
	var payload map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &payload)
	}
	if payload == nil {
		payload = map[string]any{}
	}
	return res.StatusCode, payload
}

func (e *environment) waitJobStatus(t *testing.T, id, status string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job := e.get(t, id)
		if job["status"] == status {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("job %s status=%v want %s", id, e.get(t, id)["status"], status)
}
