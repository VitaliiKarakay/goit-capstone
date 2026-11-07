package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	MaxCampaignBodyBytes  = 1 << 20
	MinCampaignRecipients = 1
	MaxCampaignRecipients = 1000
)

type CampaignRecipientPrepared struct {
	RecipientID string
	To          string
	Variables   map[string]string
	Payload     json.RawMessage
}

type NewCampaign struct {
	IdempotencyKey  string
	ContentHash     []byte
	ScheduledAt     time.Time
	Sender          string
	TemplateSubject string
	TemplateBody    string
	Recipients      []CampaignRecipientPrepared
}

type CampaignSummary struct {
	ID          uuid.UUID
	ScheduledAt time.Time
	Sender      string
	TotalJobs   int
	Counts      map[Status]int
}

type RecipientCancelOutcome struct {
	RecipientID string
	Result      string
}

const (
	CampaignCancelResultCanceled        = "canceled"
	CampaignCancelResultAlreadyCanceled = "already_canceled"
	CampaignCancelResultTooLate         = "too_late"
	CampaignCancelResultNotFound        = "not_found"
)

var ErrCampaignNotFound = errors.New("campaign not found")

func CampaignJobIdempotencyKey(campaignID uuid.UUID, recipientID string) string {
	return "campaign:" + campaignID.String() + ":recipient:" + recipientID
}

func ParseCampaign(body []byte, now time.Time) (NewCampaign, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var req struct {
		IdempotencyKey string `json:"idempotency_key"`
		ScheduledAt    string `json:"scheduled_at"`
		Sender         string `json:"sender"`
		Template       struct {
			Subject string `json:"subject"`
			Body    string `json:"body"`
		} `json:"template"`
		Recipients []struct {
			RecipientID string                     `json:"recipient_id"`
			To          string                     `json:"to"`
			Variables   map[string]json.RawMessage `json:"variables"`
		} `json:"recipients"`
	}
	if err := dec.Decode(&req); err != nil {
		if strings.Contains(err.Error(), "unknown field") {
			return NewCampaign{}, Validation("request validation failed", []FieldError{{Field: "body", Code: "unknown_field"}})
		}
		return NewCampaign{}, InvalidJSON("request body must be a single JSON object")
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return NewCampaign{}, InvalidJSON("request body must contain a single JSON object")
	}

	var details []FieldError
	if !idempotencyKeyPattern.MatchString(req.IdempotencyKey) {
		details = append(details, FieldError{Field: "idempotency_key", Code: "invalid"})
	}
	scheduledAt, ok := parseScheduledAt(req.ScheduledAt)
	if !ok {
		details = append(details, FieldError{Field: "scheduled_at", Code: "invalid_format"})
	} else if scheduledAt.After(now.Add(maxFuture)) {
		details = append(details, FieldError{Field: "scheduled_at", Code: "too_far"})
	}
	if req.Sender != "mock" {
		details = append(details, FieldError{Field: "sender", Code: "unsupported"})
	}
	subject := strings.TrimSpace(req.Template.Subject)
	bodyText := strings.TrimSpace(req.Template.Body)
	if subject == "" {
		details = append(details, FieldError{Field: "template.subject", Code: "required"})
	} else if len(subject) > MaxTemplateFieldBytes {
		details = append(details, FieldError{Field: "template.subject", Code: "too_large"})
	}
	if bodyText == "" {
		details = append(details, FieldError{Field: "template.body", Code: "required"})
	} else if len(bodyText) > MaxTemplateFieldBytes {
		details = append(details, FieldError{Field: "template.body", Code: "too_large"})
	}
	details = append(details, ScanTemplateSyntax("template.subject", "template.body", subject, bodyText)...)

	if len(req.Recipients) < MinCampaignRecipients {
		details = append(details, FieldError{Field: "recipients", Code: "too_few"})
	} else if len(req.Recipients) > MaxCampaignRecipients {
		details = append(details, FieldError{Field: "recipients", Code: "too_many"})
	}

	seen := map[string]int{}
	prepared := make([]CampaignRecipientPrepared, 0, len(req.Recipients))
	for i, rec := range req.Recipients {
		prefix := "recipients[" + strconv.Itoa(i) + "]"
		if !validRecipient(rec.RecipientID) {
			details = append(details, FieldError{Field: prefix + ".recipient_id", Code: "invalid"})
		} else if _, dup := seen[rec.RecipientID]; dup {
			details = append(details, FieldError{Field: prefix + ".recipient_id", Code: "duplicate"})
		} else {
			seen[rec.RecipientID] = i
		}
		to := strings.TrimSpace(rec.To)
		if to == "" || utf8.RuneCountInString(to) > 256 {
			details = append(details, FieldError{Field: prefix + ".to", Code: "invalid"})
		}
		vars := map[string]string{}
		if rec.Variables != nil {
			for k, raw := range rec.Variables {
				if !isPlaceholderName(k) {
					details = append(details, FieldError{Field: prefix + ".variables." + k, Code: "invalid"})
					continue
				}
				var text string
				if err := json.Unmarshal(raw, &text); err != nil {
					details = append(details, FieldError{Field: prefix + ".variables." + k, Code: "invalid"})
					continue
				}
				vars[k] = text
			}
		}
		prepared = append(prepared, CampaignRecipientPrepared{
			RecipientID: rec.RecipientID,
			To:          to,
			Variables:   vars,
		})
	}

	if len(details) > 0 {
		return NewCampaign{}, Validation("request validation failed", details)
	}

	for i := range prepared {
		prefix := "recipients[" + strconv.Itoa(i) + "]"
		subj, bod, errFields := RenderTemplate(
			prefix+".template.subject", prefix+".template.body",
			subject, bodyText, prepared[i].Variables,
		)
		if len(errFields) > 0 {
			return NewCampaign{}, Validation("request validation failed", errFields)
		}
		payloadObj := map[string]string{"to": prepared[i].To, "subject": subj, "body": bod}
		raw, err := json.Marshal(payloadObj)
		if err != nil {
			return NewCampaign{}, InvalidJSON("payload must be valid JSON")
		}
		payloadErrs := payloadErrors(raw)
		for _, fe := range payloadErrs {
			fe.Field = prefix + "." + fe.Field
			return NewCampaign{}, Validation("request validation failed", []FieldError{fe})
		}
		prepared[i].Payload = raw
	}

	hash, err := CampaignContentHash(FormatTime(scheduledAt), req.Sender, subject, bodyText, prepared)
	if err != nil {
		return NewCampaign{}, InvalidJSON("request validation failed")
	}

	return NewCampaign{
		IdempotencyKey:  req.IdempotencyKey,
		ContentHash:     hash,
		ScheduledAt:     scheduledAt,
		Sender:          req.Sender,
		TemplateSubject: subject,
		TemplateBody:    bodyText,
		Recipients:      prepared,
	}, nil
}

func ParseCampaignCancel(body []byte) ([]string, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var req struct {
		RecipientIDs []string `json:"recipient_ids"`
	}
	if err := dec.Decode(&req); err != nil {
		if strings.Contains(err.Error(), "unknown field") {
			return nil, Validation("request validation failed", []FieldError{{Field: "body", Code: "unknown_field"}})
		}
		return nil, InvalidJSON("request body must be a single JSON object")
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, InvalidJSON("request body must contain a single JSON object")
	}
	var details []FieldError
	if len(req.RecipientIDs) == 0 {
		details = append(details, FieldError{Field: "recipient_ids", Code: "required"})
	} else if len(req.RecipientIDs) > MaxCampaignRecipients {
		details = append(details, FieldError{Field: "recipient_ids", Code: "too_many"})
	}
	seen := map[string]struct{}{}
	for i, id := range req.RecipientIDs {
		if !validRecipient(id) {
			details = append(details, FieldError{Field: "recipient_ids[" + strconv.Itoa(i) + "]", Code: "invalid"})
		}
		if _, dup := seen[id]; dup {
			details = append(details, FieldError{Field: "recipient_ids[" + strconv.Itoa(i) + "]", Code: "duplicate"})
		} else {
			seen[id] = struct{}{}
		}
	}
	if len(details) > 0 {
		return nil, Validation("request validation failed", details)
	}
	return req.RecipientIDs, nil
}
