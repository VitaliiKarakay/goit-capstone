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

func TestHandleSkipsCanceled(t *testing.T) {
	job := sampleJob()
	job.Status = domain.StatusCanceled
	store := &fakeStore{found: true, job: job}
	send := &fakeSender{}
	msg := &fakeMsg{data: []byte(job.ID.String())}
	testRunner(store, send).handle(context.Background(), msg.data, msg)
	if send.calls != 0 || msg.acks != 1 {
		t.Fatalf("calls=%d acks=%d", send.calls, msg.acks)
	}
}

func TestHandleRetriesMarkAfterClaimWithoutSendingEarly(t *testing.T) {
	job := sampleJob()
	store := &fakeStore{found: true, claimed: true, job: job, markErr: dialErr()}
	send := &fakeSender{}
	msg := &fakeMsg{data: []byte(job.ID.String())}
	runner := testRunner(store, send)
	runner.retryDelays = nil

	runner.handle(context.Background(), msg.data, msg)
	if send.calls != 0 || store.marked || msg.acks != 0 || len(msg.naks) != 1 {
		t.Fatalf("calls=%d marked=%v acks=%d naks=%v", send.calls, store.marked, msg.acks, msg.naks)
	}

	store.claimed = false
	store.markErr = nil
	store.job.Status = domain.StatusProcessing
	again := &fakeMsg{data: msg.data}
	runner.handle(context.Background(), again.data, again)
	if send.calls != 1 || !store.marked || store.finished != domain.StatusSucceeded || again.acks != 1 {
		t.Fatalf("calls=%d marked=%v finish=%s acks=%d", send.calls, store.marked, store.finished, again.acks)
	}
}

func TestHandleContinuesProcessingBeforeSendOnAnotherWorker(t *testing.T) {
	job := sampleJob()
	job.Status = domain.StatusProcessing
	store := &fakeStore{found: true, job: job}
	send := &fakeSender{}
	msg := &fakeMsg{data: []byte(job.ID.String())}
	testRunner(store, send).handle(context.Background(), msg.data, msg)
	if send.calls != 1 || !store.marked || store.finished != domain.StatusSucceeded || msg.acks != 1 {
		t.Fatalf("calls=%d marked=%v finish=%s acks=%d", send.calls, store.marked, store.finished, msg.acks)
	}
}

func TestHandleRetriesFinishWithoutRedelivery(t *testing.T) {
	job := sampleJob()
	store := &fakeStore{found: true, claimed: true, job: job, finishFails: 2}
	send := &fakeSender{err: errors.New("mock rejected")}
	msg := &fakeMsg{data: []byte(job.ID.String())}
	runner := testRunner(store, send)
	runner.retryDelays = nil
	runner.nakDelay = time.Millisecond

	runner.handle(context.Background(), msg.data, msg)
	if send.calls != 1 || !store.marked || store.finished != domain.StatusFailed || store.last == nil || *store.last != "mock rejected" {
		t.Fatalf("calls=%d marked=%v finish=%s last=%v", send.calls, store.marked, store.finished, store.last)
	}
	if msg.acks != 1 || len(msg.naks) != 0 {
		t.Fatalf("acks=%d naks=%v", msg.acks, msg.naks)
	}
}

func TestHandleMarkSendStartedSurvivesLostResponse(t *testing.T) {
	job := sampleJob()
	store := &fakeStore{found: true, claimed: true, job: job, spuriousMarkErr: true}
	send := &fakeSender{}
	msg := &fakeMsg{data: []byte(job.ID.String())}
	runner := testRunner(store, send)
	runner.retryDelays = []time.Duration{0}

	runner.handle(context.Background(), msg.data, msg)
	if send.calls != 1 || !store.marked || store.finished != domain.StatusSucceeded || msg.acks != 1 {
		t.Fatalf("calls=%d marked=%v finish=%s acks=%d", send.calls, store.marked, store.finished, msg.acks)
	}
}

func TestHandleDelaysProcessingWhenSendAlreadyStarted(t *testing.T) {
	job := sampleJob()
	started := time.Now().UTC()
	job.Status = domain.StatusProcessing
	job.SendStartedAt = &started
	store := &fakeStore{found: true, job: job}
	send := &fakeSender{}
	msg := &fakeMsg{data: []byte(job.ID.String())}
	runner := testRunner(store, send)
	runner.retryDelays = nil

	runner.handle(context.Background(), msg.data, msg)
	if send.calls != 0 || store.finished != "" || msg.acks != 0 || len(msg.naks) != 1 || msg.naks[0] != NakDelay {
		t.Fatalf("calls=%d finish=%s acks=%d naks=%v", send.calls, store.finished, msg.acks, msg.naks)
	}
}

func TestHandleSkipsAttemptWhenNotOwner(t *testing.T) {
	job := sampleJob()
	job.Status = domain.StatusProcessing
	owner := "worker-a"
	job.WorkerID = &owner
	store := &fakeStore{found: true, job: job}
	send := &fakeSender{}
	msg := &fakeMsg{data: []byte(job.ID.String())}
	runner := testRunner(store, send)
	runner.workerID = "worker-b"
	runner.retryDelays = nil

	runner.handle(context.Background(), msg.data, msg)
	if send.calls != 0 || store.marked || msg.acks != 0 || len(msg.naks) != 1 {
		t.Fatalf("calls=%d marked=%v acks=%d naks=%v", send.calls, store.marked, msg.acks, msg.naks)
	}
	if runner.Stats.NotOwnerRejects.Load() != 1 {
		t.Fatalf("not owner rejects = %d", runner.Stats.NotOwnerRejects.Load())
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
	attempt := uuid.New()
	owner := "worker-test"
	return domain.Job{
		ID:            uuid.New(),
		RecipientID:   "user-1",
		Status:        domain.StatusQueued,
		Payload:       []byte(`{"to":"a","subject":"s","body":"b"}`),
		SendAttemptID: &attempt,
		WorkerID:      &owner,
	}
}

func dialErr() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
}

type fakeStore struct {
	claimErr        error
	markErr         error
	spuriousMarkErr bool
	finishFails     int
	finishCalls     int
	job             domain.Job
	found           bool
	claimed         bool
	marked          bool
	finished        domain.Status
	last            *string
}

func (f *fakeStore) Claim(context.Context, uuid.UUID, string) (domain.Job, bool, bool, error) {
	if f.claimErr != nil {
		return domain.Job{}, false, false, f.claimErr
	}
	return f.job, f.found, f.claimed, nil
}

func (f *fakeStore) MarkSendStarted(_ context.Context, id, attempt uuid.UUID) (time.Time, error) {
	if f.job.SendAttemptID == nil || attempt != *f.job.SendAttemptID {
		return time.Time{}, domain.ErrNotProcessing
	}
	if f.markErr != nil {
		return time.Time{}, f.markErr
	}
	if f.marked {
		return time.Now().UTC(), nil
	}
	f.marked = true
	if f.spuriousMarkErr {
		f.spuriousMarkErr = false
		return time.Now().UTC(), dialErr()
	}
	return time.Now().UTC(), nil
}

func (f *fakeStore) Finish(_ context.Context, _ uuid.UUID, status domain.Status, lastError *string) error {
	f.finishCalls++
	if f.finishFails > 0 && f.finishCalls <= f.finishFails {
		return dialErr()
	}
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

func (m *fakeMsg) InProgress(...nats.AckOpt) error {
	return nil
}
