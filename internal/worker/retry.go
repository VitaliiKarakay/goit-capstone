package worker

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
)

func retryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, domain.ErrNotProcessing) || errors.Is(err, domain.ErrNotFound) {
		return false
	}
	if pgconn.SafeToRetry(err) || pgconn.Timeout(err) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case strings.HasPrefix(pgErr.Code, "08"), pgErr.Code == "57P01", pgErr.Code == "57P02", pgErr.Code == "57P03", pgErr.Code == "40001", pgErr.Code == "40P01":
			return true
		}
	}
	return false
}

func retryDB(ctx context.Context, delays []time.Duration, op func() error) error {
	var err error
	for attempt := 0; ; attempt++ {
		err = op()
		if err == nil || !retryable(err) || attempt >= len(delays) {
			return err
		}
		timer := time.NewTimer(delays[attempt])
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
