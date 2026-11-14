package metrics

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// JobStoreSnapshot reads aggregated job/outbox counts for metrics.
type JobStoreSnapshot interface {
	MetricsSnapshot(ctx context.Context) (DBCounts, error)
}

// DBCounts is a point-in-time database aggregate.
type DBCounts struct {
	ByStatus          map[string]int
	ScheduledFuture   int
	ScheduledOverdue  int
	OutboxUnpublished int
}

// DBSnapshot refreshes database gauges on an interval and exposes them atomically.
type DBSnapshot struct {
	log     *slog.Logger
	store   JobStoreSnapshot
	refresh time.Duration
	queryTO time.Duration

	mu sync.RWMutex
	// published is swapped only on successful refresh.
	published   *publishedSnap
	success     prometheus.Gauge
	lastSuccess prometheus.Gauge
	failures    prometheus.Counter
}

type publishedSnap struct {
	counts      DBCounts
	collectedAt time.Time
}

// DBCollector implements prometheus.Collector for cached job/outbox gauges.
type DBCollector struct {
	parent *DBSnapshot
}

func NewDBSnapshot(log *slog.Logger, reg prometheus.Registerer, store JobStoreSnapshot, refresh, queryTimeout time.Duration) *DBSnapshot {
	if refresh <= 0 {
		refresh = 10 * time.Second
	}
	if queryTimeout <= 0 {
		queryTimeout = 2 * time.Second
	}
	s := &DBSnapshot{
		log:     log,
		store:   store,
		refresh: refresh,
		queryTO: queryTimeout,
		success: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "scheduler_db_snapshot_success",
			Help: "1 if the last database metrics refresh succeeded.",
		}),
		lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "scheduler_db_snapshot_last_success_timestamp_seconds",
			Help: "Unix time of the last successful database metrics refresh.",
		}),
		failures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_db_snapshot_failures_total",
			Help: "Failed database metrics refresh attempts.",
		}),
	}
	reg.MustRegister(s.success, s.lastSuccess, s.failures)
	reg.MustRegister(&DBCollector{parent: s})
	return s
}

func (s *DBSnapshot) Run(ctx context.Context) {
	s.tick(ctx)
	ticker := time.NewTicker(s.refresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *DBSnapshot) tick(ctx context.Context) {
	qctx, cancel := context.WithTimeout(ctx, s.queryTO)
	defer cancel()
	counts, err := s.store.MetricsSnapshot(qctx)
	if err != nil {
		s.log.Error("db metrics snapshot", "err", err)
		s.success.Set(0)
		s.failures.Inc()
		return
	}
	now := time.Now()
	s.mu.Lock()
	s.published = &publishedSnap{counts: counts, collectedAt: now}
	s.mu.Unlock()
	s.success.Set(1)
	s.lastSuccess.Set(float64(now.Unix()))
}

func (s *DBSnapshot) snapshot() (*publishedSnap, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.published == nil {
		return nil, false
	}
	return s.published, true
}

func (c *DBCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("scheduler_jobs_by_status", "Jobs per status from the last successful DB snapshot.", []string{"status"}, nil)
	ch <- prometheus.NewDesc("scheduler_jobs_scheduled_future", "Scheduled jobs with scheduled_at in the future.", nil, nil)
	ch <- prometheus.NewDesc("scheduler_jobs_scheduled_overdue", "Scheduled jobs past scheduled_at not yet queued.", nil, nil)
	ch <- prometheus.NewDesc("scheduler_outbox_unpublished", "Outbox rows not yet published.", nil, nil)
}

func (c *DBCollector) Collect(ch chan<- prometheus.Metric) {
	snap, ok := c.parent.snapshot()
	if !ok {
		return
	}
	statusDesc := prometheus.NewDesc("scheduler_jobs_by_status", "Jobs per status from the last successful DB snapshot.", []string{"status"}, nil)
	for status, n := range snap.counts.ByStatus {
		ch <- prometheus.MustNewConstMetric(statusDesc, prometheus.GaugeValue, float64(n), status)
	}
	ch <- prometheus.MustNewConstMetric(prometheus.NewDesc("scheduler_jobs_scheduled_future", "Scheduled jobs with scheduled_at in the future.", nil, nil), prometheus.GaugeValue, float64(snap.counts.ScheduledFuture))
	ch <- prometheus.MustNewConstMetric(prometheus.NewDesc("scheduler_jobs_scheduled_overdue", "Scheduled jobs past scheduled_at not yet queued.", nil, nil), prometheus.GaugeValue, float64(snap.counts.ScheduledOverdue))
	ch <- prometheus.MustNewConstMetric(prometheus.NewDesc("scheduler_outbox_unpublished", "Outbox rows not yet published.", nil, nil), prometheus.GaugeValue, float64(snap.counts.OutboxUnpublished))
}
