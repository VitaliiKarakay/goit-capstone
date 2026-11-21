package domain

import (
	"strconv"
)

const (
	DefaultListLimit = 20
	MaxListLimit     = 100
)

type PageParams struct {
	Limit  int
	Offset int
}

func ParsePageQuery(limitRaw, offsetRaw string) (PageParams, []FieldError) {
	limit := DefaultListLimit
	offset := 0
	var details []FieldError
	if limitRaw != "" {
		v, err := strconv.Atoi(limitRaw)
		if err != nil || v < 1 {
			details = append(details, FieldError{Field: "limit", Code: "invalid"})
		} else if v > MaxListLimit {
			details = append(details, FieldError{Field: "limit", Code: "too_large"})
		} else {
			limit = v
		}
	}
	if offsetRaw != "" {
		v, err := strconv.Atoi(offsetRaw)
		if err != nil || v < 0 {
			details = append(details, FieldError{Field: "offset", Code: "invalid"})
		} else {
			offset = v
		}
	}
	if len(details) > 0 {
		return PageParams{}, details
	}
	return PageParams{Limit: limit, Offset: offset}, nil
}

func ParseStatusFilter(raw string) (Status, []FieldError) {
	if raw == "" {
		return "", nil
	}
	switch Status(raw) {
	case StatusScheduled, StatusQueued, StatusProcessing, StatusSucceeded, StatusFailed, StatusCanceled, StatusUnknown:
		return Status(raw), nil
	default:
		return "", []FieldError{{Field: "status", Code: "invalid"}}
	}
}
