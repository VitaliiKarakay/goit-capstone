package sender

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
)

type fixedRand struct {
	v float64
}

func (f fixedRand) Float64() float64 { return f.v }

func TestConfigurableMockSuccessAndFailure(t *testing.T) {
	ctx := context.Background()
	job := Job{ID: uuid.New(), RecipientID: "r1"}
	m := ConfigurableMock{
		Logger: slog.New(slog.DiscardHandler),
		Rand:   fixedRand{v: 0.5},
	}
	if err := m.Send(ctx, job); err != nil {
		t.Fatal(err)
	}
	m.FailureRate = 1
	if !errors.Is(m.Send(ctx, job), ErrSimulatedSend) {
		t.Fatal("expected simulated failure")
	}
}

func TestConfigurableMockDelayRespectsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	m := ConfigurableMock{
		Logger: slog.New(slog.DiscardHandler),
		Delay:  time.Hour,
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	job := Job{ID: uuid.New()}
	err := m.Send(ctx, job)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}

func TestConfigurableMockZeroDelayChecksContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m := ConfigurableMock{Logger: slog.New(slog.DiscardHandler)}
	if !errors.Is(m.Send(ctx, Job{ID: uuid.New()}), context.Canceled) {
		t.Fatal("expected canceled")
	}
}
