package metrics

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/VitaliiKarakay/goit-capstone/internal/sender"
)

// Worker holds sender-side Prometheus metrics for one worker process.
type Worker struct {
	Invocations *prometheus.CounterVec
	Duration    prometheus.Histogram
	StartLag    prometheus.Histogram
	InFlight    prometheus.Gauge
}

func NewWorker(reg prometheus.Registerer) *Worker {
	w := &Worker{
		Invocations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "scheduler_sender_invocations_total",
			Help: "Completed mock sender Send calls (not real delivery).",
		}, []string{"result"}),
		Duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "scheduler_sender_call_duration_seconds",
			Help:    "Wall-clock duration of mock sender Send calls.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}),
		StartLag: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "scheduler_sender_start_lag_seconds",
			Help:    "send_started_at minus scheduled_at for jobs that reached Send (DB timestamps).",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800},
		}),
		InFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "scheduler_sender_calls_in_flight",
			Help: "Mock sender Send calls currently executing in this process.",
		}),
	}
	reg.MustRegister(w.Invocations, w.Duration, w.StartLag, w.InFlight)
	return w
}

// Instrumented wraps a sender and records mock invocation metrics.
type Instrumented struct {
	Inner  sender.Sender
	Worker *Worker
}

func (i Instrumented) Send(ctx context.Context, job sender.Job) error {
	i.Worker.InFlight.Inc()
	defer i.Worker.InFlight.Dec()

	if !job.SendStartedAt.IsZero() && !job.ScheduledAt.IsZero() {
		lag := job.SendStartedAt.Sub(job.ScheduledAt).Seconds()
		if lag >= 0 {
			i.Worker.StartLag.Observe(lag)
		}
	}

	start := time.Now()
	err := i.Inner.Send(ctx, job)
	i.Worker.Duration.Observe(time.Since(start).Seconds())

	result := classifySendErr(ctx, err)
	i.Worker.Invocations.WithLabelValues(result).Inc()
	return err
}

func classifySendErr(ctx context.Context, err error) string {
	if err == nil {
		return "success"
	}
	if errors.Is(err, sender.ErrSimulatedSend) {
		return "simulated_error"
	}
	if ctx.Err() != nil {
		return "context_canceled"
	}
	return "error"
}
