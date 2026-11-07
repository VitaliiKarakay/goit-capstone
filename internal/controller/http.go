package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
)

const (
	MaxBodyBytes         = 64 * 1024
	MaxCampaignBodyBytes = domain.MaxCampaignBodyBytes
)

type JobRepository interface {
	CreateJob(ctx context.Context, job domain.NewJob) (domain.Job, bool, error)
	GetJob(ctx context.Context, id uuid.UUID) (domain.Job, error)
	CancelJob(ctx context.Context, id uuid.UUID) (domain.Job, domain.CancelOutcome, error)
}

type CampaignRepository interface {
	CreateCampaign(ctx context.Context, in domain.NewCampaign) (domain.CampaignSummary, bool, error)
	GetCampaign(ctx context.Context, id uuid.UUID) (domain.CampaignSummary, error)
	CancelCampaignRecipients(ctx context.Context, id uuid.UUID, recipientIDs []string) ([]domain.RecipientCancelOutcome, error)
}

type Repository interface {
	JobRepository
	CampaignRepository
}

type API struct {
	log  *slog.Logger
	jobs JobRepository
	camp CampaignRepository
}

func NewAPI(log *slog.Logger, repo Repository) *API {
	return &API{log: log, jobs: repo, camp: repo}
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", a.createJob)
	mux.HandleFunc("POST /jobs/{id}/cancel", a.cancelJob)
	mux.HandleFunc("GET /jobs/{id}", a.getJob)
	mux.HandleFunc("POST /campaigns", a.createCampaign)
	mux.HandleFunc("GET /campaigns/{id}", a.getCampaign)
	mux.HandleFunc("POST /campaigns/{id}/cancel", a.cancelCampaign)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

func (a *API) createJob(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			writeRequestTooLarge(w)
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_json", "could not read request body", nil)
		return
	}
	job, err := domain.ParseCreate(body, time.Now())
	if err != nil {
		writeParseError(w, err)
		return
	}
	created, inserted, err := a.jobs.CreateJob(r.Context(), job)
	if errors.Is(err, domain.ErrConflict) {
		writeError(w, http.StatusConflict, "idempotency_conflict", "idempotency key already exists with different content", nil)
		return
	}
	if err != nil {
		a.log.Error("create job", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error", nil)
		return
	}
	status := http.StatusOK
	if inserted {
		status = http.StatusCreated
	}
	w.Header().Set("Location", "/jobs/"+created.ID.String())
	writeJSON(w, status, jobView(created))
}

func (a *API) getJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", "job not found", nil)
		return
	}
	job, err := a.jobs.GetJob(r.Context(), id)
	if errors.Is(err, domain.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "job not found", nil)
		return
	}
	if err != nil {
		a.log.Error("get job", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error", nil)
		return
	}
	writeJSON(w, http.StatusOK, jobView(job))
}

func (a *API) cancelJob(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			writeRequestTooLarge(w)
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_json", "could not read request body", nil)
		return
	}
	if len(bytes.TrimSpace(body)) > 0 {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must be empty", nil)
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", "job not found", nil)
		return
	}
	job, outcome, err := a.jobs.CancelJob(r.Context(), id)
	if errors.Is(err, domain.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "job not found", nil)
		return
	}
	if errors.Is(err, domain.ErrTooLate) {
		writeError(w, http.StatusConflict, "too_late", "job can no longer be canceled", nil)
		return
	}
	if err != nil {
		a.log.Error("cancel job", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error", nil)
		return
	}
	writeJSON(w, http.StatusOK, cancelResponse{
		JobID:  job.ID.String(),
		Status: string(domain.StatusCanceled),
		Result: string(outcome),
	})
}

func readBody(r *http.Request) ([]byte, error) {
	return readBodyLimit(r, MaxBodyBytes)
}

func readBodyLimit(r *http.Request, limit int64) ([]byte, error) {
	r.Body = http.MaxBytesReader(nil, r.Body, limit)
	body, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	return body, err
}

func writeCampaignTooLarge(w http.ResponseWriter) {
	writeError(w, http.StatusBadRequest, "request_too_large", "request body exceeds "+strconv.Itoa(MaxCampaignBodyBytes)+" bytes", nil)
}

func writeParseError(w http.ResponseWriter, err error) {
	var req *domain.RequestError
	if errors.As(err, &req) {
		writeError(w, http.StatusBadRequest, req.Code, req.Message, req.Details)
		return
	}
	writeError(w, http.StatusBadRequest, "invalid_json", "request body must be a single JSON object", nil)
}

func writeRequestTooLarge(w http.ResponseWriter) {
	writeError(w, http.StatusBadRequest, "request_too_large", "request body exceeds "+strconv.Itoa(MaxBodyBytes)+" bytes", nil)
}

type errorResponse struct {
	Error errorObject `json:"error"`
}

type errorObject struct {
	Code    string              `json:"code"`
	Message string              `json:"message"`
	Details []domain.FieldError `json:"details,omitempty"`
}

func writeError(w http.ResponseWriter, status int, code, message string, details []domain.FieldError) {
	writeJSON(w, status, errorResponse{Error: errorObject{Code: code, Message: message, Details: details}})
}

func (a *API) createCampaign(w http.ResponseWriter, r *http.Request) {
	body, err := readBodyLimit(r, MaxCampaignBodyBytes)
	if err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			writeCampaignTooLarge(w)
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_json", "could not read request body", nil)
		return
	}
	in, err := domain.ParseCampaign(body, time.Now())
	if err != nil {
		writeParseError(w, err)
		return
	}
	summary, inserted, err := a.camp.CreateCampaign(r.Context(), in)
	if errors.Is(err, domain.ErrConflict) {
		writeError(w, http.StatusConflict, "idempotency_conflict", "idempotency key already exists with different content", nil)
		return
	}
	if err != nil {
		a.log.Error("create campaign", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error", nil)
		return
	}
	status := http.StatusOK
	if inserted {
		status = http.StatusCreated
	}
	w.Header().Set("Location", "/campaigns/"+summary.ID.String())
	writeJSON(w, status, campaignView(summary))
}

func (a *API) getCampaign(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", "campaign not found", nil)
		return
	}
	summary, err := a.camp.GetCampaign(r.Context(), id)
	if errors.Is(err, domain.ErrCampaignNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "campaign not found", nil)
		return
	}
	if err != nil {
		a.log.Error("get campaign", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error", nil)
		return
	}
	writeJSON(w, http.StatusOK, campaignView(summary))
}

func (a *API) cancelCampaign(w http.ResponseWriter, r *http.Request) {
	body, err := readBodyLimit(r, MaxBodyBytes)
	if err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			writeError(w, http.StatusBadRequest, "request_too_large", "request body exceeds "+strconv.Itoa(MaxBodyBytes)+" bytes", nil)
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_json", "could not read request body", nil)
		return
	}
	recipientIDs, err := domain.ParseCampaignCancel(body)
	if err != nil {
		writeParseError(w, err)
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", "campaign not found", nil)
		return
	}
	outcomes, err := a.camp.CancelCampaignRecipients(r.Context(), id, recipientIDs)
	if errors.Is(err, domain.ErrCampaignNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "campaign not found", nil)
		return
	}
	if err != nil {
		a.log.Error("cancel campaign", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error", nil)
		return
	}
	results := make([]campaignCancelResult, len(outcomes))
	for i, o := range outcomes {
		results[i] = campaignCancelResult{RecipientID: o.RecipientID, Result: o.Result}
	}
	writeJSON(w, http.StatusOK, campaignCancelResponse{CampaignID: id.String(), Results: results})
}

type jobResponse struct {
	ID             string          `json:"id"`
	IdempotencyKey string          `json:"idempotency_key"`
	CampaignID     *string         `json:"campaign_id,omitempty"`
	RecipientID    string          `json:"recipient_id"`
	ScheduledAt    string          `json:"scheduled_at"`
	Sender         string          `json:"sender"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	WorkerID       *string         `json:"worker_id"`
	LastError      *string         `json:"last_error"`
	SendStartedAt  *string         `json:"send_started_at"`
	CreatedAt      string          `json:"created_at"`
	UpdatedAt      string          `json:"updated_at"`
	FinishedAt     *string         `json:"finished_at"`
	CanceledAt     *string         `json:"canceled_at,omitempty"`
}

type cancelResponse struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
	Result string `json:"result"`
}

type campaignResponse struct {
	CampaignID  string         `json:"campaign_id"`
	ScheduledAt string         `json:"scheduled_at"`
	Sender      string         `json:"sender"`
	TotalJobs   int            `json:"total_jobs"`
	Counts      map[string]int `json:"counts"`
}

type campaignCancelResponse struct {
	CampaignID string                 `json:"campaign_id"`
	Results    []campaignCancelResult `json:"results"`
}

type campaignCancelResult struct {
	RecipientID string `json:"recipient_id"`
	Result      string `json:"result"`
}

func campaignView(c domain.CampaignSummary) campaignResponse {
	counts := map[string]int{
		string(domain.StatusScheduled):  c.Counts[domain.StatusScheduled],
		string(domain.StatusQueued):     c.Counts[domain.StatusQueued],
		string(domain.StatusProcessing): c.Counts[domain.StatusProcessing],
		string(domain.StatusSucceeded):  c.Counts[domain.StatusSucceeded],
		string(domain.StatusFailed):     c.Counts[domain.StatusFailed],
		string(domain.StatusCanceled):   c.Counts[domain.StatusCanceled],
	}
	return campaignResponse{
		CampaignID:  c.ID.String(),
		ScheduledAt: domain.FormatTime(c.ScheduledAt),
		Sender:      c.Sender,
		TotalJobs:   c.TotalJobs,
		Counts:      counts,
	}
}

func jobView(job domain.Job) jobResponse {
	var campaignID *string
	if job.CampaignID != nil {
		value := job.CampaignID.String()
		campaignID = &value
	}
	return jobResponse{
		ID:             job.ID.String(),
		IdempotencyKey: job.IdempotencyKey,
		CampaignID:     campaignID,
		RecipientID:    job.RecipientID,
		ScheduledAt:    domain.FormatTime(job.ScheduledAt),
		Sender:         job.Sender,
		Payload:        job.Payload,
		Status:         string(job.Status),
		WorkerID:       job.WorkerID,
		LastError:      job.LastError,
		SendStartedAt:  formatOptional(job.SendStartedAt),
		CreatedAt:      domain.FormatTime(job.CreatedAt),
		UpdatedAt:      domain.FormatTime(job.UpdatedAt),
		FinishedAt:     formatOptional(job.FinishedAt),
		CanceledAt:     formatOptional(job.CanceledAt),
	}
}

func formatOptional(t *time.Time) *string {
	if t == nil {
		return nil
	}
	value := domain.FormatTime(*t)
	return &value
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}
