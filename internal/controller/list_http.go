package controller

import (
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
)

func (a *API) listJobs(w http.ResponseWriter, r *http.Request) {
	page, details := domain.ParsePageQuery(r.URL.Query().Get("limit"), r.URL.Query().Get("offset"))
	if len(details) > 0 {
		writeError(w, http.StatusBadRequest, "validation_error", "request validation failed", details)
		return
	}
	status, details := domain.ParseStatusFilter(r.URL.Query().Get("status"))
	if len(details) > 0 {
		writeError(w, http.StatusBadRequest, "validation_error", "request validation failed", details)
		return
	}
	result, err := a.jobs.ListStandaloneJobs(r.Context(), status, page)
	if err != nil {
		a.log.Error("list jobs", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error", nil)
		return
	}
	writeJSON(w, http.StatusOK, jobListView(result))
}

func (a *API) listCampaigns(w http.ResponseWriter, r *http.Request) {
	page, details := domain.ParsePageQuery(r.URL.Query().Get("limit"), r.URL.Query().Get("offset"))
	if len(details) > 0 {
		writeError(w, http.StatusBadRequest, "validation_error", "request validation failed", details)
		return
	}
	result, err := a.camp.ListCampaigns(r.Context(), page)
	if err != nil {
		a.log.Error("list campaigns", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error", nil)
		return
	}
	items := make([]campaignResponse, len(result.Items))
	for i, c := range result.Items {
		items[i] = campaignListView(c)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"total":  result.Total,
		"limit":  result.Limit,
		"offset": result.Offset,
	})
}

func (a *API) listCampaignJobs(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", "campaign not found", nil)
		return
	}
	page, details := domain.ParsePageQuery(r.URL.Query().Get("limit"), r.URL.Query().Get("offset"))
	if len(details) > 0 {
		writeError(w, http.StatusBadRequest, "validation_error", "request validation failed", details)
		return
	}
	status, details := domain.ParseStatusFilter(r.URL.Query().Get("status"))
	if len(details) > 0 {
		writeError(w, http.StatusBadRequest, "validation_error", "request validation failed", details)
		return
	}
	result, err := a.camp.ListCampaignJobs(r.Context(), id, status, page)
	if errors.Is(err, domain.ErrCampaignNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "campaign not found", nil)
		return
	}
	if err != nil {
		a.log.Error("list campaign jobs", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error", nil)
		return
	}
	writeJSON(w, http.StatusOK, campaignJobListView(result))
}

type jobListItemResponse struct {
	ID          string `json:"id"`
	RecipientID string `json:"recipient_id"`
	Status      string `json:"status"`
	ScheduledAt string `json:"scheduled_at"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

func jobListView(page domain.JobListPage) map[string]any {
	items := make([]jobListItemResponse, len(page.Items))
	for i, j := range page.Items {
		items[i] = jobListItemResponse{
			ID:          j.ID.String(),
			RecipientID: j.RecipientID,
			Status:      string(j.Status),
			ScheduledAt: domain.FormatTime(j.ScheduledAt),
			CreatedAt:   domain.FormatTime(j.CreatedAt),
			UpdatedAt:   domain.FormatTime(j.UpdatedAt),
		}
	}
	return map[string]any{
		"items":  items,
		"total":  page.Total,
		"limit":  page.Limit,
		"offset": page.Offset,
	}
}

func campaignListView(c domain.CampaignSummary) campaignResponse {
	resp := campaignView(c)
	resp.Template = nil
	resp.CreatedAt = ""
	return resp
}

type campaignJobItemResponse struct {
	JobID       string  `json:"job_id"`
	RecipientID string  `json:"recipient_id"`
	Status      string  `json:"status"`
	ScheduledAt string  `json:"scheduled_at"`
	LastError   *string `json:"last_error"`
	CanceledAt  *string `json:"canceled_at,omitempty"`
	FinishedAt  *string `json:"finished_at,omitempty"`
}

func campaignJobListView(page domain.CampaignJobListPage) map[string]any {
	items := make([]campaignJobItemResponse, len(page.Items))
	for i, j := range page.Items {
		items[i] = campaignJobItemResponse{
			JobID:       j.JobID.String(),
			RecipientID: j.RecipientID,
			Status:      string(j.Status),
			ScheduledAt: domain.FormatTime(j.ScheduledAt),
			LastError:   j.LastError,
			CanceledAt:  formatOptional(j.CanceledAt),
			FinishedAt:  formatOptional(j.FinishedAt),
		}
	}
	return map[string]any{
		"items":  items,
		"total":  page.Total,
		"limit":  page.Limit,
		"offset": page.Offset,
	}
}
