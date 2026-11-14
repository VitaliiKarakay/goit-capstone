//go:build integration

package itest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestListStandaloneJobsExcludesCampaign(t *testing.T) {
	at := time.Now().Add(time.Hour).UTC()
	_, _, err := env.post(jobBody("list-solo-"+uuid.NewString(), "solo-list", at, "solo"))
	if err != nil {
		t.Fatal(err)
	}
	camp := campaignBody("list-camp-"+uuid.NewString(), at, []recipientSpec{
		{"camp-u", "c@example.com", campaignVars("C", "1", "UAH")},
	})
	if _, _, err := env.postCampaign(camp); err != nil {
		t.Fatal(err)
	}
	res, err := http.Get(env.api + "/jobs?limit=100")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	items, _ := body["items"].([]any)
	for _, raw := range items {
		row := raw.(map[string]any)
		id := row["id"].(string)
		var campID *string
		err := env.db.QueryRow(context.Background(), `SELECT campaign_id::text FROM jobs WHERE id = $1::uuid`, id).Scan(&campID)
		if err != nil {
			t.Fatal(err)
		}
		if campID != nil && *campID != "" {
			t.Fatalf("campaign job in standalone list %s", id)
		}
	}
}

func TestListCampaignsBatchCounts(t *testing.T) {
	at := time.Now().Add(2 * time.Hour).UTC()
	code, resp, err := env.postCampaign(campaignBody("list-c-"+uuid.NewString(), at, []recipientSpec{
		{"a", "a@example.com", campaignVars("A", "1", "UAH")},
		{"b", "b@example.com", campaignVars("B", "2", "UAH")},
	}))
	if err != nil || code != http.StatusCreated {
		t.Fatal(err, code)
	}
	id := resp["campaign_id"].(string)
	res, err := http.Get(env.api + "/campaigns?limit=20")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var page map[string]any
	_ = json.NewDecoder(res.Body).Decode(&page)
	found := false
	for _, raw := range page["items"].([]any) {
		row := raw.(map[string]any)
		if row["campaign_id"] == id {
			found = true
			if int(row["total_jobs"].(float64)) != 2 {
				t.Fatalf("total_jobs=%v", row["total_jobs"])
			}
			counts := row["counts"].(map[string]any)
			if int(counts["scheduled"].(float64)) != 2 {
				t.Fatalf("counts=%v", counts)
			}
		}
	}
	if !found {
		t.Fatal("campaign not in list")
	}
}

func TestListCampaignJobsSorted(t *testing.T) {
	at := time.Now().Add(3 * time.Hour).UTC()
	_, resp, _ := env.postCampaign(campaignBody("list-j-"+uuid.NewString(), at, []recipientSpec{
		{"z-last", "z@example.com", campaignVars("Z", "1", "UAH")},
		{"a-first", "a@example.com", campaignVars("A", "2", "UAH")},
	}))
	cid := resp["campaign_id"].(string)
	res, err := http.Get(env.api + "/campaigns/" + cid + "/jobs")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var page map[string]any
	_ = json.NewDecoder(res.Body).Decode(&page)
	items := page["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("items=%d", len(items))
	}
	if items[0].(map[string]any)["recipient_id"] != "a-first" {
		t.Fatalf("order %v", items)
	}
}

func (e *environment) apiGet(path string) (int, map[string]any) {
	res, err := http.Get(e.api + path)
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var payload map[string]any
	_ = json.Unmarshal(raw, &payload)
	return res.StatusCode, payload
}

func TestGetCampaignIncludesTemplate(t *testing.T) {
	at := time.Now().Add(4 * time.Hour).UTC()
	body := campaignBody("tpl-"+uuid.NewString(), at, []recipientSpec{
		{"u", "u@example.com", campaignVars("U", "1", "UAH")},
	})
	body = strings.Replace(body, "[name], offer", "[name], special", 1)
	_, resp, _ := env.postCampaign(body)
	cid := resp["campaign_id"].(string)
	code, detail := env.apiGet("/campaigns/" + cid)
	if code != 200 {
		t.Fatalf("%d %v", code, detail)
	}
	tpl, ok := detail["template"].(map[string]any)
	if !ok || tpl["subject"] == "" {
		t.Fatalf("template=%v", detail["template"])
	}
}
