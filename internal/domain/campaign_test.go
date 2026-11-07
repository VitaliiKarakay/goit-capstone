package domain

import (
	"strings"
	"testing"
	"time"
)

func TestParseCampaignRejectsNullVariable(t *testing.T) {
	body := `{
		"idempotency_key":"camp-null",
		"scheduled_at":"2099-01-01T12:00:00Z",
		"sender":"mock",
		"template":{"subject":"[name]","body":"[sum] [currency]"},
		"recipients":[{"recipient_id":"u1","to":"a@b.c","variables":{"name":null,"sum":"1","currency":"UAH"}}]
	}`
	_, err := ParseCampaign([]byte(body), time.Now())
	req, ok := err.(*RequestError)
	if !ok || req.Code != "validation_error" {
		t.Fatalf("err=%v", err)
	}
	found := false
	for _, d := range req.Details {
		if strings.Contains(d.Field, "variables.name") && d.Code == "invalid" {
			found = true
		}
	}
	if !found {
		t.Fatalf("details=%v", req.Details)
	}
}
