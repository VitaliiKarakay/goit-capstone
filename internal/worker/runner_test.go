package worker

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
	"github.com/VitaliiKarakay/goit-capstone/internal/sender"
)

func TestDeliveryBudgetSurvivesAShortOutage(t *testing.T) {
	var inProcess time.Duration
	for _, delay := range DBRetryDelays {
		inProcess += delay
	}
	if inProcess < 3*time.Second {
		t.Fatalf("in-process retry budget = %s", inProcess)
	}
	if NakDelay < time.Second {
		t.Fatalf("nak delay = %s", NakDelay)
	}
	if time.Duration(MaxDeliver)*NakDelay < time.Minute {
		t.Fatalf("max deliver %d with nak %s is exhausted by an outage under a minute", MaxDeliver, NakDelay)
	}
	if AckWait <= inProcess {
		t.Fatal("ack wait must outlast the in-process database retries")
	}
}

func TestHandleRecordsFailureWithoutAnotherSend(t *testing.T) {
	store := &fakeStore{found: true, claimed: true, job: sampleJob()}
	send := &fakeSender{err: errors.New("mock rejected")}
	msg := &fakeMsg{data: []byte(store.job.ID.String())}
	runner := testRunner(store, send)

	runner.handle(context.Background(), msg.data, msg)

	if send.calls != 1 {
		t.Fatalf("calls = %d", send.calls)
	}
	if !store.marked {
		t.Fatal("send started without a timestamp")
	}
	if store.finished != domain.StatusFailed || store.last == nil || *store.last != "mock rejected" {
		t.Fatalf("finish = %s %v", store.finished, store.last)
	}
	if msg.acks != 1 || len(msg.naks) != 0 {
		t.Fatalf("acks=%d naks=%v", msg.acks, msg.naks)
	}
}

func TestHandleSkipsTerminalDuplicate(t *testing.T) {
	job := sampleJob()
	job.Status = domain.StatusSucceeded
	store := &fakeStore{found: true, job: job}
	send := &fakeSender{}
	msg := &fakeMsg{data: []byte(job.ID.String())}
	testRunner(store, send).handle(context.Background(), msg.data, msg)
	if send.calls != 0 || msg.acks != 1 {
		t.Fatalf("calls=%d acks=%d", send.calls, msg.acks)
	}
}

func TestHandleDelaysWhenDatabaseIsDown(t *testing.T) {
	store := &fakeStore{claimErr: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}}
	send := &fakeSender{}
	id := uuid.New()
	msg := &fakeMsg{data: []byte(id.String())}
	runner := testRunner(store, send)
	runner.retryDelays = nil

	runner.handle(context.Background(), msg.data, msg)

	if send.calls != 0 || msg.acks != 0 || len(msg.naks) != 1 || msg.naks[0] != NakDelay {
		t.Fatalf("calls=%d acks=%d naks=%v", send.calls, msg.acks, msg.naks)
	}
	if runner.Stats.DelayedNak.Load() != 1 {
		t.Fatalf("naks = %d", runner.Stats.DelayedNak.Load())
	}
}

func testRunner(store Store, send sender.Sender) *Runner {
	return &Runner{
		log:         slog.New(slog.DiscardHandler),
		store:       store,
		sender:      send,
		workerID:    "worker-test",
		retryDelays: DBRetryDelays,
		nakDelay:    NakDelay,
	}
}

func sampleJob() domain.Job {
	return domain.Job{ID: uuid.New(), RecipientID: "user-1", Status: domain.StatusQueued, Payload: []byte(`{"to":"a","subject":"s","body":"b"}`)}
}

type fakeStore struct {
	claimErr error
	job      domain.Job
	found    bool
	claimed  bool
	marked   bool
	finished domain.Status
	last     *string
}

func (f *fakeStore) Claim(context.Context, uuid.UUID, string) (domain.Job, bool, bool, error) {
	if f.claimErr != nil {
		return domain.Job{}, false, false, f.claimErr
	}
	return f.job, f.found, f.claimed, nil
}

func (f *fakeStore) MarkSendStarted(context.Context, uuid.UUID) (time.Time, error) {
	f.marked = true
	return time.Now().UTC(), nil
}

func (f *fakeStore) Finish(_ context.Context, _ uuid.UUID, status domain.Status, lastError *string) error {
	f.finished = status
	f.last = lastError
	return nil
}

type fakeSender struct {
	err   error
	calls int
}

func (f *fakeSender) Send(context.Context, sender.Job) error {
	f.calls++
	return f.err
}

type fakeMsg struct {
	data []byte
	acks int
	naks []time.Duration
}

func (m *fakeMsg) Ack(...nats.AckOpt) error {
	m.acks++
	return nil
}

func (m *fakeMsg) NakWithDelay(delay time.Duration, _ ...nats.AckOpt) error {
	m.naks = append(m.naks, delay)
	return nil
}
