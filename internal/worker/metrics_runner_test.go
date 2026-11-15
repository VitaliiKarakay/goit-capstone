package worker

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
	"github.com/VitaliiKarakay/goit-capstone/internal/metrics"
)

func TestRunnerMetricsSkipsCanceledWithoutSend(t *testing.T) {
	reg := prometheus.NewRegistry()
	wm := metrics.NewWorker(reg)
	send := &fakeSender{}
	runner := newInstrumentedRunner(wm, send)

	job := sampleJob()
	job.Status = domain.StatusCanceled
	runner.store = &fakeStore{found: true, job: job}

	msg := &fakeMsg{data: []byte(job.ID.String())}
	runner.handle(context.Background(), msg.data, msg)
	assertNoSenderMetrics(t, reg, wm, send)
}

func TestRunnerMetricsSkipsTerminalDuplicateWithoutSend(t *testing.T) {
	reg := prometheus.NewRegistry()
	wm := metrics.NewWorker(reg)
	send := &fakeSender{}
	runner := newInstrumentedRunner(wm, send)

	job := sampleJob()
	job.Status = domain.StatusSucceeded
	runner.store = &fakeStore{found: true, job: job}

	msg := &fakeMsg{data: []byte(job.ID.String())}
	runner.handle(context.Background(), msg.data, msg)
	assertNoSenderMetrics(t, reg, wm, send)
}

func TestRunnerMetricsSingleSendDespiteFinishRetries(t *testing.T) {
	reg := prometheus.NewRegistry()
	wm := metrics.NewWorker(reg)
	send := &fakeSender{}
	runner := newInstrumentedRunner(wm, send)

	job := sampleJob()
	job.ScheduledAt = time.Now().Add(-time.Minute).UTC()
	runner.store = &fakeStore{found: true, claimed: true, job: job, finishFails: 2}
	runner.retryDelays = nil
	runner.nakDelay = time.Millisecond

	msg := &fakeMsg{data: []byte(job.ID.String())}
	runner.handle(context.Background(), msg.data, msg)

	if send.calls != 1 {
		t.Fatalf("send calls=%d", send.calls)
	}
	if v := testutil.ToFloat64(wm.Invocations.WithLabelValues("success")); v != 1 {
		t.Fatalf("success invocations=%v", v)
	}
	if histogramSamples(t, reg, "scheduler_sender_call_duration_seconds") != 1 {
		t.Fatal("expected one duration sample")
	}
	if histogramSamples(t, reg, "scheduler_sender_start_lag_seconds") != 1 {
		t.Fatal("expected one start-lag sample")
	}
}

func newInstrumentedRunner(wm *metrics.Worker, send *fakeSender) *Runner {
	inst := metrics.Instrumented{Inner: send, Worker: wm}
	r := testRunner(&fakeStore{}, inst)
	return r
}

func assertNoSenderMetrics(t *testing.T, reg *prometheus.Registry, wm *metrics.Worker, send *fakeSender) {
	t.Helper()
	if send.calls != 0 {
		t.Fatalf("send calls=%d", send.calls)
	}
	for _, result := range []string{"success", "simulated_error", "context_canceled", "error"} {
		if v := testutil.ToFloat64(wm.Invocations.WithLabelValues(result)); v != 0 {
			t.Fatalf("result %s=%v", result, v)
		}
	}
	if histogramSamples(t, reg, "scheduler_sender_call_duration_seconds") != 0 {
		t.Fatal("unexpected duration samples")
	}
	if histogramSamples(t, reg, "scheduler_sender_start_lag_seconds") != 0 {
		t.Fatal("unexpected start-lag samples")
	}
}

func histogramSamples(t *testing.T, reg *prometheus.Registry, name string) uint64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		var total uint64
		for _, m := range mf.GetMetric() {
			if h := m.GetHistogram(); h != nil {
				total += h.GetSampleCount()
			}
		}
		return total
	}
	return 0
}
