package domain

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	MaxPayloadBytes = 16 * 1024
	maxFuture       = 366 * 24 * time.Hour
)

var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

func ParseCreate(body []byte, now time.Time) (NewJob, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var req struct {
		IdempotencyKey string          `json:"idempotency_key"`
		RecipientID    string          `json:"recipient_id"`
		ScheduledAt    string          `json:"scheduled_at"`
		Sender         string          `json:"sender"`
		Payload        json.RawMessage `json:"payload"`
	}
	if err := dec.Decode(&req); err != nil {
		if strings.Contains(err.Error(), "unknown field") {
			return NewJob{}, Validation("request validation failed", []FieldError{{Field: "body", Code: "unknown_field"}})
		}
		return NewJob{}, InvalidJSON("request body must be a single JSON object")
	}
	if dec.More() {
		return NewJob{}, InvalidJSON("request body must contain a single JSON object")
	}

	var details []FieldError
	if !idempotencyKeyPattern.MatchString(req.IdempotencyKey) {
		details = append(details, FieldError{Field: "idempotency_key", Code: "invalid"})
	}
	if !validRecipient(req.RecipientID) {
		details = append(details, FieldError{Field: "recipient_id", Code: "invalid"})
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
	details = append(details, payloadErrors(req.Payload)...)
	if len(details) > 0 {
		return NewJob{}, Validation("request validation failed", details)
	}

	hash, err := ContentHash(req.RecipientID, req.Sender, scheduledAt, req.Payload)
	if err != nil {
		return NewJob{}, InvalidJSON("payload must be valid JSON")
	}
	return NewJob{
		IdempotencyKey: req.IdempotencyKey,
		ContentHash:    hash,
		RecipientID:    req.RecipientID,
		ScheduledAt:    scheduledAt,
		Sender:         req.Sender,
		Payload:        append(json.RawMessage(nil), req.Payload...),
	}, nil
}

func parseScheduledAt(value string) (time.Time, bool) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, false
	}
	return NormalizeTime(parsed), true
}

func validRecipient(value string) bool {
	if value == "" || utf8.RuneCountInString(value) > 128 {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func payloadErrors(raw json.RawMessage) []FieldError {
	if len(bytes.TrimSpace(raw)) == 0 {
		return []FieldError{{Field: "payload", Code: "required"}}
	}
	if len(raw) > MaxPayloadBytes {
		return []FieldError{{Field: "payload", Code: "too_large"}}
	}
	if bytes.TrimSpace(raw)[0] != '{' {
		return []FieldError{{Field: "payload", Code: "invalid"}}
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return []FieldError{{Field: "payload", Code: "invalid"}}
	}
	var details []FieldError
	for _, name := range []string{"to", "subject", "body"} {
		value, ok := fields[name]
		if !ok || string(bytes.TrimSpace(value)) == "null" {
			details = append(details, FieldError{Field: "payload." + name, Code: "required"})
			continue
		}
		var text string
		if err := json.Unmarshal(value, &text); err != nil || text == "" {
			details = append(details, FieldError{Field: "payload." + name, Code: "invalid"})
		}
	}
	return details
}
