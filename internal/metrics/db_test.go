package metrics

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type fakeSnapStore struct {
	counts DBCounts
	err    error
	calls  int
}

func (f *fakeSnapStore) MetricsSnapshot(context.Context) (DBCounts, error) {
	f.calls++
	if f.err != nil {
		return DBCounts{}, f.err
	}
	return f.counts, nil
}

func TestDBSnapshotNoMetricsBeforeFirstSuccess(t *testing.T) {
	reg := prometheus.NewRegistry()
	store := &fakeSnapStore{
		counts: DBCounts{
			ByStatus:          map[string]int{"queued": 3},
			ScheduledOverdue:  1,
			OutboxUnpublished: 2,
		},
	}
	snap := NewDBSnapshot(slog.New(slog.DiscardHandler), reg, store, time.Hour, time.Second)

	if hasJobStatusMetric(reg) {
		t.Fatal("unexpected job metrics before refresh")
	}
	snap.tick(context.Background())
	if !hasJobStatusMetric(reg) {
		t.Fatal("expected job metrics after success")
	}
	if v := gaugeValue(reg, "scheduler_jobs_by_status", "queued"); v != 3 {
		t.Fatalf("queued=%v", v)
	}
	if v := testutil.ToFloat64(snap.success); v != 1 {
		t.Fatal()
	}
}

func TestDBSnapshotFailureKeepsLastGood(t *testing.T) {
	reg := prometheus.NewRegistry()
	store := &fakeSnapStore{
		counts: DBCounts{ByStatus: map[string]int{"queued": 5}},
	}
	snap := NewDBSnapshot(slog.New(slog.DiscardHandler), reg, store, time.Hour, time.Second)
	snap.tick(context.Background())
	store.err = errors.New("db down")
	snap.tick(context.Background())
	if v := gaugeValue(reg, "scheduler_jobs_by_status", "queued"); v != 5 {
		t.Fatalf("queued=%v", v)
	}
	if v := testutil.ToFloat64(snap.success); v != 0 {
		t.Fatal()
	}
}

func hasJobStatusMetric(reg *prometheus.Registry) bool {
	mfs, err := reg.Gather()
	if err != nil {
		return false
	}
	for _, mf := range mfs {
		if mf.GetName() == "scheduler_jobs_by_status" {
			return true
		}
	}
	return false
}

func gaugeValue(reg *prometheus.Registry, name, label string) float64 {
	mfs, err := reg.Gather()
	if err != nil {
		return -1
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			ok := label == ""
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "status" && lp.GetValue() == label {
					ok = true
				}
			}
			if ok && m.GetGauge() != nil {
				return m.GetGauge().GetValue()
			}
		}
	}
	return -1
}
