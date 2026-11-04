package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
)

func TestCreateRejectsOversizedBodyBeforeDecode(t *testing.T) {
	api := NewAPI(slog.New(slog.DiscardHandler), &memStore{})
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(bytes.Repeat([]byte("a"), MaxBodyBytes+1)))
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "request_too_large") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestCreateRejectsSecondJSONValue(t *testing.T) {
	body := validBody(time.Now().Add(time.Hour)) + `{"another":true}`
	status, raw := post(t, body)
	if status != http.StatusBadRequest || !strings.Contains(raw, "invalid_json") {
		t.Fatalf("status=%d body=%s", status, raw)
	}
}

func TestCreateAcceptsPastTimeAndReplaysIdempotency(t *testing.T) {
	store := &memStore{}
	api := NewAPI(slog.New(slog.DiscardHandler), store)
	server := httptest.NewServer(api.Handler())
	defer server.Close()

	at := time.Now().Add(-time.Hour).UTC().Truncate(time.Microsecond)
	body := validBody(at)
	res := do(t, server.URL, body)
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", res.StatusCode, res.raw)
	}
	if store.saved.ScheduledAt.After(time.Now()) {
		t.Fatal("past scheduled_at was moved into the future")
	}
	replay := do(t, server.URL, body)
	if replay.StatusCode != http.StatusOK || replay.job["id"] != res.job["id"] {
		t.Fatalf("replay status=%d id=%v", replay.StatusCode, replay.job["id"])
	}
	changed := strings.Replace(body, `"subject":"s"`, `"subject":"other"`, 1)
	conflict := do(t, server.URL, changed)
	if conflict.StatusCode != http.StatusConflict || !strings.Contains(conflict.raw, "idempotency_conflict") {
		t.Fatalf("conflict status=%d body=%s", conflict.StatusCode, conflict.raw)
	}
}

func TestGetUnknownJob(t *testing.T) {
	api := NewAPI(slog.New(slog.DiscardHandler), &memStore{})
	req := httptest.NewRequest(http.MethodGet, "/jobs/"+uuid.NewString(), nil)
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d", rec.Code)
	}
}

func post(t *testing.T, body string) (int, string) {
	t.Helper()
	api := NewAPI(slog.New(slog.DiscardHandler), &memStore{})
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

type httpResult struct {
	StatusCode int
	raw        string
	job        map[string]any
}

func do(t *testing.T, base, body string) httpResult {
	t.Helper()
	res, err := http.Post(base+"/jobs", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	_ = json.Unmarshal(raw, &payload)
	return httpResult{StatusCode: res.StatusCode, raw: string(raw), job: payload}
}

func validBody(at time.Time) string {
	body := map[string]any{
		"idempotency_key": "key-1",
		"recipient_id":    "user-1",
		"scheduled_at":    at.Format(time.RFC3339Nano),
		"sender":          "mock",
		"payload":         map[string]string{"to": "a@b.c", "subject": "s", "body": "b"},
	}
	raw, _ := json.Marshal(body)
	return string(raw)
}

type memStore struct {
	saved domain.Job
	hash  []byte
}

func (m *memStore) CreateJob(_ context.Context, in domain.NewJob) (domain.Job, bool, error) {
	if m.saved.ID != uuid.Nil {
		if !bytes.Equal(m.hash, in.ContentHash) {
			return domain.Job{}, false, domain.ErrConflict
		}
		return m.saved, false, nil
	}
	m.hash = append([]byte(nil), in.ContentHash...)
	m.saved = domain.Job{
		ID:             uuid.New(),
		IdempotencyKey: in.IdempotencyKey,
		RecipientID:    in.RecipientID,
		ScheduledAt:    in.ScheduledAt,
		Sender:         in.Sender,
		Payload:        in.Payload,
		Status:         domain.StatusScheduled,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}
	return m.saved, true, nil
}

func (m *memStore) GetJob(_ context.Context, id uuid.UUID) (domain.Job, error) {
	if m.saved.ID != id {
		return domain.Job{}, domain.ErrNotFound
	}
	return m.saved, nil
}
