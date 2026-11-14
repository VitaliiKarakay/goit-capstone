package metrics

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/VitaliiKarakay/goit-capstone/internal/sender"
)

type stubSender struct {
	err error
}

func (s stubSender) Send(context.Context, sender.Job) error { return s.err }

func TestInstrumentedMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	wm := NewWorker(reg)
	ctx := context.Background()
	scheduled := time.Now().Add(-time.Minute)
	started := time.Now()
	job := sender.Job{
		ID:            uuid.New(),
		ScheduledAt:   scheduled,
		SendStartedAt: started,
	}

	inst := Instrumented{Inner: stubSender{}, Worker: wm}
	if err := inst.Send(ctx, job); err != nil {
		t.Fatal(err)
	}
	if v := testutil.ToFloat64(wm.Invocations.WithLabelValues("success")); v != 1 {
		t.Fatalf("success=%v", v)
	}

	inst = Instrumented{Inner: stubSender{err: sender.ErrSimulatedSend}, Worker: wm}
	_ = inst.Send(ctx, job)
	if v := testutil.ToFloat64(wm.Invocations.WithLabelValues("simulated_error")); v != 1 {
		t.Fatal()
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	inst = Instrumented{Inner: stubSender{err: errors.New("boom")}, Worker: wm}
	_ = inst.Send(cctx, job)
	if v := testutil.ToFloat64(wm.Invocations.WithLabelValues("context_canceled")); v != 1 {
		t.Fatal()
	}

	inst = Instrumented{Inner: stubSender{err: errors.New("other")}, Worker: wm}
	_ = inst.Send(ctx, job)
	if v := testutil.ToFloat64(wm.Invocations.WithLabelValues("error")); v != 1 {
		t.Fatal()
	}
}

func TestInstrumentedInFlight(t *testing.T) {
	reg := prometheus.NewRegistry()
	wm := NewWorker(reg)
	block := make(chan struct{})
	inst := Instrumented{Inner: instrumentedSlow{block: block}, Worker: wm}
	go func() {
		_ = inst.Send(context.Background(), sender.Job{ID: uuid.New()})
	}()
	time.Sleep(30 * time.Millisecond)
	if v := testutil.ToFloat64(wm.InFlight); v < 1 {
		t.Fatalf("in flight=%v", v)
	}
	close(block)
	time.Sleep(20 * time.Millisecond)
	if v := testutil.ToFloat64(wm.InFlight); v != 0 {
		t.Fatalf("in flight=%v", v)
	}
}

type instrumentedSlow struct {
	block chan struct{}
}

func (s instrumentedSlow) Send(ctx context.Context, job sender.Job) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.block:
		return nil
	}
}

func TestTerminalSkipNoWrapperCall(t *testing.T) {
	// Instrumented only runs when Send is invoked from runner; this test documents counter isolation.
	reg := prometheus.NewRegistry()
	wm := NewWorker(reg)
	if v := testutil.ToFloat64(wm.Invocations.WithLabelValues("success")); v != 0 {
		t.Fatal()
	}
}

func TestRegistryServe(t *testing.T) {
	reg := prometheus.NewRegistry()
	wm := NewWorker(reg)
	wm.Invocations.WithLabelValues("success").Add(0)
	rec := httptest.NewRecorder()
	Handler(reg).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "scheduler_sender_invocations_total") {
		t.Fatal("missing metric")
	}
}
