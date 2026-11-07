package domain

import (
	"crypto/sha256"
	"encoding/json"
	"slices"
)

type campaignHashRecipient struct {
	RecipientID string            `json:"recipient_id"`
	To          string            `json:"to"`
	Variables   map[string]string `json:"variables"`
}

func CampaignContentHash(scheduledAt string, sender, subject, body string, recipients []CampaignRecipientPrepared) ([]byte, error) {
	rows := make([]campaignHashRecipient, len(recipients))
	for i, r := range recipients {
		vars := make(map[string]string, len(r.Variables))
		for k, v := range r.Variables {
			vars[k] = v
		}
		rows[i] = campaignHashRecipient{
			RecipientID: r.RecipientID,
			To:          r.To,
			Variables:   vars,
		}
	}
	slices.SortFunc(rows, func(a, b campaignHashRecipient) int {
		return stringsCompare(a.RecipientID, b.RecipientID)
	})
	for i := range rows {
		keys := make([]string, 0, len(rows[i].Variables))
		for k := range rows[i].Variables {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		sorted := make(map[string]string, len(keys))
		for _, k := range keys {
			sorted[k] = rows[i].Variables[k]
		}
		rows[i].Variables = sorted
	}
	payload := map[string]any{
		"scheduled_at": scheduledAt,
		"sender":       sender,
		"template": map[string]string{
			"subject": subject,
			"body":    body,
		},
		"recipients": rows,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	canon, err := CanonicalJSON(raw)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(canon)
	return sum[:], nil
}

func stringsCompare(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
