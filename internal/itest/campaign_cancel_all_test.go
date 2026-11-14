//go:build integration

package itest

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCancelAllScheduledCampaign(t *testing.T) {
	at := time.Now().Add(time.Hour).UTC()
	_, resp, err := env.postCampaign(campaignBody("cancel-all-sch-"+uuid.NewString(), at, []recipientSpec{
		{"u1", "1@example.com", campaignVars("A", "1", "UAH")},
		{"u2", "2@example.com", campaignVars("B", "2", "UAH")},
	}))
	if err != nil {
		t.Fatal(err)
	}
	cid := resp["campaign_id"].(string)
	code, out := env.cancelCampaignAll(cid)
	if code != http.StatusOK || out["canceled_now"].(float64) != 2 {
		t.Fatalf("cancel-all %d %v", code, out)
	}
	assertCancelAllSum(t, out)
	summary := env.getCampaign(t, cid)
	if summary["counts"].(map[string]any)["canceled"].(float64) != 2 {
		t.Fatalf("summary=%v", summary)
	}
}

func TestCancelAllQueuedNoSend(t *testing.T) {
	env.fault.pauseClaim.Store(true)
	t.Cleanup(func() { env.fault.pauseClaim.Store(false) })

	at := time.Now().Add(-time.Second).UTC()
	_, resp, err := env.postCampaign(campaignBody("cancel-all-q-"+uuid.NewString(), at, []recipientSpec{
		{"u1", "1@example.com", campaignVars("A", "1", "UAH")},
	}))
	if err != nil {
		t.Fatal(err)
	}
	cid := resp["campaign_id"].(string)
	env.waitCampaignQueued(t, cid, 1, 20*time.Second)
	code, out := env.cancelCampaignAll(cid)
	if code != http.StatusOK || out["canceled_now"].(float64) != 1 {
		t.Fatal(out)
	}
	env.fault.pauseClaim.Store(false)
	time.Sleep(2 * time.Second)
	if env.rec.countByCampaign(t, cid) != 0 {
		t.Fatal("sender was called")
	}
}

func TestCancelAllMixedCounts(t *testing.T) {
	at := time.Now().Add(time.Hour).UTC()
	_, resp, err := env.postCampaign(campaignBody("cancel-all-mix-"+uuid.NewString(), at, []recipientSpec{
		{"u1", "1@example.com", campaignVars("A", "1", "UAH")},
		{"u2", "2@example.com", campaignVars("B", "2", "UAH")},
		{"u3", "3@example.com", campaignVars("C", "3", "UAH")},
	}))
	if err != nil {
		t.Fatal(err)
	}
	cid := resp["campaign_id"].(string)
	if code, _ := env.cancelCampaign(cid, []string{"u1"}); code != http.StatusOK {
		t.Fatal("targeted cancel")
	}
	code, out := env.cancelCampaignAll(cid)
	if code != http.StatusOK {
		t.Fatal(out)
	}
	if int(out["canceled_now"].(float64)) != 2 || int(out["already_canceled"].(float64)) != 1 {
		t.Fatalf("%v", out)
	}
	assertCancelAllSum(t, out)
}

func TestCancelAllReplayZero(t *testing.T) {
	at := time.Now().Add(time.Hour).UTC()
	_, resp, _ := env.postCampaign(campaignBody("cancel-all-rep-"+uuid.NewString(), at, []recipientSpec{
		{"u1", "1@example.com", campaignVars("A", "1", "UAH")},
	}))
	cid := resp["campaign_id"].(string)
	env.cancelCampaignAll(cid)
	code, out := env.cancelCampaignAll(cid)
	if code != http.StatusOK || out["canceled_now"].(float64) != 0 {
		t.Fatalf("%d %v", code, out)
	}
}

func TestCancelAllUnknownCampaign(t *testing.T) {
	code, _ := env.cancelCampaignAll(uuid.NewString())
	if code != http.StatusNotFound {
		t.Fatalf("code=%d", code)
	}
}

func TestCancelAllRejectsBody(t *testing.T) {
	at := time.Now().Add(time.Hour).UTC()
	_, resp, _ := env.postCampaign(campaignBody("cancel-all-body-"+uuid.NewString(), at, []recipientSpec{
		{"u1", "1@example.com", campaignVars("A", "1", "UAH")},
	}))
	cid := resp["campaign_id"].(string)
	req, err := http.NewRequest(http.MethodPost, env.api+"/campaigns/"+cid+"/cancel-all", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d", res.StatusCode)
	}
}

func TestCancelAllRacesClaim(t *testing.T) {
	env.fault.pauseClaim.Store(true)
	t.Cleanup(func() { env.fault.pauseClaim.Store(false) })

	at := time.Now().Add(-time.Second).UTC()
	_, resp, _ := env.postCampaign(campaignBody("cancel-all-race-"+uuid.NewString(), at, []recipientSpec{
		{"u1", "1@example.com", campaignVars("A", "1", "UAH")},
		{"u2", "2@example.com", campaignVars("B", "2", "UAH")},
	}))
	cid := resp["campaign_id"].(string)
	env.waitCampaignQueued(t, cid, 2, 20*time.Second)

	release := make(chan struct{})
	var wg sync.WaitGroup
	var code int
	var out map[string]any
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-release
		code, out = env.cancelCampaignAll(cid)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-release
		env.fault.pauseClaim.Store(false)
	}()
	close(release)
	wg.Wait()
	time.Sleep(2 * time.Second)
	if code != http.StatusOK {
		t.Fatalf("%v", out)
	}
	assertCancelAllSum(t, out)
	if env.rec.countByCampaign(t, cid) > int(out["too_late"].(float64)) {
		t.Fatalf("unexpected sends out=%v calls=%d", out, env.rec.countByCampaign(t, cid))
	}
}

func TestConcurrentCancelAllAndTargeted(t *testing.T) {
	at := time.Now().Add(time.Hour).UTC()
	_, resp, err := env.postCampaign(campaignBody("cancel-all-tgt-"+uuid.NewString(), at, []recipientSpec{
		{"u1", "1@example.com", campaignVars("A", "1", "UAH")},
		{"u2", "2@example.com", campaignVars("B", "2", "UAH")},
		{"u3", "3@example.com", campaignVars("C", "3", "UAH")},
		{"u4", "4@example.com", campaignVars("D", "4", "UAH")},
	}))
	if err != nil {
		t.Fatal(err)
	}
	cid := resp["campaign_id"].(string)

	release := make(chan struct{})
	var wg sync.WaitGroup
	var allCode, tgtCode int
	var allOut, tgtOut map[string]any
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-release
		allCode, allOut = env.cancelCampaignAll(cid)
	}()
	go func() {
		defer wg.Done()
		<-release
		tgtCode, tgtOut = env.cancelCampaign(cid, []string{"u1", "u2", "u3", "u4"})
	}()
	close(release)

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent cancel-all and targeted cancel hung")
	}

	if allCode != http.StatusOK || tgtCode != http.StatusOK {
		t.Fatalf("all=%d %v tgt=%d %v", allCode, allOut, tgtCode, tgtOut)
	}
	assertCancelAllSum(t, allOut)
	assertTargetedCancelPending(t, tgtOut, []string{"u1", "u2", "u3", "u4"})
	if int(allOut["total_jobs"].(float64)) != 4 || int(allOut["too_late"].(float64)) != 0 {
		t.Fatalf("cancel-all %v", allOut)
	}

	summary := env.getCampaign(t, cid)
	counts := summary["counts"].(map[string]any)
	if int(counts["canceled"].(float64)) != 4 {
		t.Fatalf("final summary=%v", summary)
	}
	for _, st := range []string{"scheduled", "queued", "processing"} {
		if int(counts[st].(float64)) != 0 {
			t.Fatalf("status %s=%v", st, counts[st])
		}
	}
}

func TestConcurrentCancelAll(t *testing.T) {
	at := time.Now().Add(time.Hour).UTC()
	_, resp, _ := env.postCampaign(campaignBody("cancel-all-conc-"+uuid.NewString(), at, []recipientSpec{
		{"u1", "1@example.com", campaignVars("A", "1", "UAH")},
		{"u2", "2@example.com", campaignVars("B", "2", "UAH")},
	}))
	cid := resp["campaign_id"].(string)
	var wg sync.WaitGroup
	codes := make([]int, 4)
	outs := make([]map[string]any, 4)
	for i := range codes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i], outs[i] = env.cancelCampaignAll(cid)
		}(i)
	}
	wg.Wait()
	totalNow := 0
	for i, code := range codes {
		if code != http.StatusOK {
			t.Fatalf("code=%d", code)
		}
		totalNow += int(outs[i]["canceled_now"].(float64))
		assertCancelAllSum(t, outs[i])
	}
	if totalNow != 2 {
		t.Fatalf("canceled_now total=%d", totalNow)
	}
}

func TestCancelAllDoesNotTouchOtherCampaigns(t *testing.T) {
	at := time.Now().Add(time.Hour).UTC()
	_, soloID, err := env.post(jobBody("solo-ca-"+uuid.NewString(), "solo", at, "solo"))
	if err != nil {
		t.Fatal(err)
	}
	_, victimResp, err := env.postCampaign(campaignBody("victim-ca-"+uuid.NewString(), at, []recipientSpec{
		{"v1", "v1@example.com", campaignVars("V1", "1", "UAH")},
		{"v2", "v2@example.com", campaignVars("V2", "2", "UAH")},
	}))
	if err != nil {
		t.Fatal(err)
	}
	victimID := victimResp["campaign_id"].(string)
	_, targetResp, err := env.postCampaign(campaignBody("target-ca-"+uuid.NewString(), at, []recipientSpec{
		{"t1", "t1@example.com", campaignVars("T1", "1", "UAH")},
		{"t2", "t2@example.com", campaignVars("T2", "2", "UAH")},
	}))
	if err != nil {
		t.Fatal(err)
	}
	targetID := targetResp["campaign_id"].(string)

	code, out := env.cancelCampaignAll(targetID)
	if code != http.StatusOK || int(out["canceled_now"].(float64)) != 2 {
		t.Fatalf("target cancel-all %d %v", code, out)
	}
	assertCancelAllSum(t, out)

	victim := env.getCampaign(t, victimID)
	vCounts := victim["counts"].(map[string]any)
	if int(vCounts["canceled"].(float64)) != 0 || int(vCounts["scheduled"].(float64)) != 2 {
		t.Fatalf("victim campaign changed: %v", victim)
	}
	target := env.getCampaign(t, targetID)
	tCounts := target["counts"].(map[string]any)
	if int(tCounts["canceled"].(float64)) != 2 {
		t.Fatalf("target not fully canceled: %v", target)
	}
	if env.get(t, soloID)["status"] != "scheduled" {
		t.Fatalf("standalone job changed: %v", env.get(t, soloID))
	}
}

func assertTargetedCancelPending(t *testing.T, out map[string]any, want []string) {
	t.Helper()
	raw, ok := out["results"].([]any)
	if !ok || len(raw) != len(want) {
		t.Fatalf("results=%v", out)
	}
	byID := make(map[string]string, len(raw))
	for _, item := range raw {
		row, _ := item.(map[string]any)
		id, _ := row["recipient_id"].(string)
		res, _ := row["result"].(string)
		byID[id] = res
	}
	for _, id := range want {
		switch byID[id] {
		case "canceled", "already_canceled":
		default:
			t.Fatalf("recipient %s result=%q full=%v", id, byID[id], out)
		}
	}
}

func assertCancelAllSum(t *testing.T, out map[string]any) {
	t.Helper()
	sum := int(out["canceled_now"].(float64)) + int(out["already_canceled"].(float64)) + int(out["too_late"].(float64))
	if sum != int(out["total_jobs"].(float64)) {
		t.Fatalf("sum mismatch %v", out)
	}
}

func (e *environment) cancelCampaignAll(id string) (int, map[string]any) {
	req, err := http.NewRequest(http.MethodPost, e.api+"/campaigns/"+id+"/cancel-all", nil)
	if err != nil {
		panic(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var payload map[string]any
	_ = json.Unmarshal(raw, &payload)
	if payload == nil {
		payload = map[string]any{}
	}
	return res.StatusCode, payload
}
