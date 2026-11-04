package domain

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

const TimeLayout = "2006-01-02T15:04:05.000000Z"

type Status string

const (
	StatusScheduled  Status = "scheduled"
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusSucceeded  Status = "succeeded"
	StatusFailed     Status = "failed"
)

var (
	ErrNotFound      = errors.New("job not found")
	ErrConflict      = errors.New("idempotency conflict")
	ErrNotProcessing = errors.New("job is not processing")
)

// NormalizeTime truncates to microseconds in UTC. PostgreSQL timestamptz
// stores microseconds, so the hashed instant and the stored instant must
// use the same truncation. Extra digits from the request are discarded, not rounded.
func NormalizeTime(t time.Time) time.Time {
	return t.UTC().Truncate(time.Microsecond)
}

func FormatTime(t time.Time) string {
	return NormalizeTime(t).Format(TimeLayout)
}

type Job struct {
	ID             uuid.UUID
	IdempotencyKey string
	RecipientID    string
	ScheduledAt    time.Time
	Sender         string
	Payload        json.RawMessage
	Status         Status
	WorkerID       *string
	LastError      *string
	SendStartedAt  *time.Time
	SendAttemptID  *uuid.UUID
	CreatedAt      time.Time
	UpdatedAt      time.Time
	FinishedAt     *time.Time
}

type NewJob struct {
	IdempotencyKey string
	ContentHash    []byte
	RecipientID    string
	ScheduledAt    time.Time
	Sender         string
	Payload        json.RawMessage
}

type FieldError struct {
	Field string `json:"field"`
	Code  string `json:"code"`
}

type RequestError struct {
	Code    string
	Message string
	Details []FieldError
}

func (e *RequestError) Error() string { return e.Message }

func InvalidJSON(message string) *RequestError {
	return &RequestError{Code: "invalid_json", Message: message}
}

func Validation(message string, details []FieldError) *RequestError {
	return &RequestError{Code: "validation_error", Message: message, Details: details}
}
