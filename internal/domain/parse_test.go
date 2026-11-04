package domain

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestContentHashIgnoresKeyOrderAndWhitespace(t *testing.T) {
	left := []byte(`{"body":"hello","subject":"s","to":"a@b.c","extra":{"z":1,"a":2}}`)
	right := []byte("{\n  \"to\": \"a@b.c\",\n  \"extra\": {\"a\": 2, \"z\": 1},\n  \"subject\": \"s\",\n  \"body\": \"hello\"\n}")
	at := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	h1, err := ContentHash("user-1", "mock", at, left)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := ContentHash("user-1", "mock", at, right)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(h1, h2) {
		t.Fatal("canonical hash changed with key order or whitespace")
	}
}

func TestContentHashKeepsNumberLiterals(t *testing.T) {
	at := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	one, err := ContentHash("user-1", "mock", at, []byte(`{"n":1}`))
	if err != nil {
		t.Fatal(err)
	}
	onePoint, err := ContentHash("user-1", "mock", at, []byte(`{"n":1.0}`))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(one, onePoint) {
		t.Fatal("1 and 1.0 hashed as the same content")
	}
}

func TestContentHashTruncatesScheduledAtToMicroseconds(t *testing.T) {
	nano := time.Date(2026, 11, 7, 12, 0, 0, 123456789, time.UTC)
	micro := time.Date(2026, 11, 7, 12, 0, 0, 123456000, time.UTC)
	offset := time.Date(2026, 11, 7, 14, 0, 0, 123456000, time.FixedZone("EEST", 2*60*60))
	payload := []byte(`{"body":"b","subject":"s","to":"a"}`)
	h1, err := ContentHash("user-1", "mock", nano, payload)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := ContentHash("user-1", "mock", micro, payload)
	if err != nil {
		t.Fatal(err)
	}
	h3, err := ContentHash("user-1", "mock", offset, payload)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(h1, h2) || !bytes.Equal(h2, h3) {
		t.Fatal("normalized instants produced different hashes")
	}
	if got := FormatTime(nano); got != "2026-11-07T12:00:00.123456Z" {
		t.Fatalf("formatted time = %s", got)
	}
}

func TestParseCreateNormalizesTimeAndAcceptsPast(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	body := []byte(`{
		"idempotency_key": "cart-reminder-123",
		"recipient_id": "Віталій",
		"scheduled_at": "2026-11-03T13:30:00.123456789+02:00",
		"sender": "mock",
		"payload": {"to": "user@example.com", "subject": "Тема", "body": "Текст"}
	}`)
	job, err := ParseCreate(body, now)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 11, 3, 11, 30, 0, 123456000, time.UTC)
	if !job.ScheduledAt.Equal(want) {
		t.Fatalf("scheduled_at = %s", job.ScheduledAt)
	}
	again, err := ParseCreate(body, now)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(job.ContentHash, again.ContentHash) {
		t.Fatal("hash was not stable")
	}
}

func TestParseCreateRejects(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	valid := `{
		"idempotency_key": "key-1",
		"recipient_id": "user-1",
		"scheduled_at": "2026-11-03T12:00:00Z",
		"sender": "mock",
		"payload": {"to": "a@b.c", "subject": "s", "body": "b"}
	}`
	cases := []struct {
		name  string
		body  string
		code  string
		field string
	}{
		{name: "trailing object", body: valid + `{"x":1}`, code: "invalid_json"},
		{name: "unknown field", body: strings.Replace(valid, `"sender"`, `"extra":1,"sender"`, 1), code: "validation_error", field: "body"},
		{name: "bad sender", body: strings.Replace(valid, `"mock"`, `"smtp"`, 1), code: "validation_error", field: "sender"},
		{name: "no zone", body: strings.Replace(valid, `"2026-11-03T12:00:00Z"`, `"2026-11-03T12:00:00"`, 1), code: "validation_error", field: "scheduled_at"},
		{name: "too far", body: strings.Replace(valid, `"2026-11-03T12:00:00Z"`, `"2028-11-03T12:00:00Z"`, 1), code: "validation_error", field: "scheduled_at"},
		{name: "payload array", body: strings.Replace(valid, `{"to": "a@b.c", "subject": "s", "body": "b"}`, `[]`, 1), code: "validation_error", field: "payload"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseCreate([]byte(tc.body), now)
			var req *RequestError
			if !errors.As(err, &req) {
				t.Fatalf("error = %v", err)
			}
			if req.Code != tc.code {
				t.Fatalf("code = %s", req.Code)
			}
			if tc.field != "" && !hasField(req.Details, tc.field) {
				t.Fatalf("details = %+v", req.Details)
			}
		})
	}
}

func TestParseCreateRejectsOversizedPayload(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	body := `{"idempotency_key":"key-1","recipient_id":"user-1","scheduled_at":"2026-11-03T12:00:00Z","sender":"mock","payload":{"to":"a","subject":"s","body":"` + strings.Repeat("x", MaxPayloadBytes) + `"}}`
	_, err := ParseCreate([]byte(body), now)
	var req *RequestError
	if !errors.As(err, &req) || !hasField(req.Details, "payload") {
		t.Fatalf("error = %v", err)
	}
}

func hasField(details []FieldError, field string) bool {
	for _, d := range details {
		if d.Field == field {
			return true
		}
	}
	return false
}
