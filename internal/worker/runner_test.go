package worker

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
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

func TestRenewHungRequestAbortsWithinLeaseBudget(t *testing.T) {
	job := sampleJob()
	leaseEnd := time.Now().UTC().Add(400 * time.Millisecond)
	job.LeaseUntil = &leaseEnd
	store := &fakeStore{found: true, claimed: true, job: job}
	store.renewHang.Store(true)
	send := &blockingSender{}
	msg := &fakeMsg{data: []byte(job.ID.String())}
	runner := testRunner(store, send)
	runner.SetLeasePolicy(400*time.Millisecond, 50*time.Millisecond, time.Minute)

	done := make(chan struct{})
	go func() {
		runner.handle(context.Background(), msg.data, msg)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler hung")
	}
	if send.calls != 1 {
		t.Fatalf("send calls=%d", send.calls)
	}
	if store.finished != "" {
		t.Fatalf("finished=%s", store.finished)
	}
	if store.renewCalls.Load() == 0 {
		t.Fatal("expected at least one renew attempt")
	}
}

func TestMultipleRenewalsDuringLongSend(t *testing.T) {
	job := sampleJob()
	leaseEnd := time.Now().UTC().Add(2 * time.Second)
	job.LeaseUntil = &leaseEnd
	store := &fakeStore{found: true, claimed: true, job: job}
	send := &blockingSender{}
	msg := &fakeMsg{data: []byte(job.ID.String())}
	runner := testRunner(store, send)
	runner.SetLeasePolicy(600*time.Millisecond, 80*time.Millisecond, time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go runner.handle(ctx, msg.data, msg)
	<-ctx.Done()
	time.Sleep(100 * time.Millisecond)
	if store.renewCalls.Load() < 2 {
		t.Fatalf("renew calls=%d want >=2", store.renewCalls.Load())
	}
}

func TestRenewLeaseLossStopsAttemptWithoutFinish(t *testing.T) {
	job := sampleJob()
	store := &fakeStore{found: true, claimed: true, job: job, renewErr: domain.ErrLeaseLost}
	send := &blockingSender{}
	msg := &fakeMsg{data: []byte(job.ID.String())}
	runner := testRunner(store, send)
	runner.SetLeasePolicy(time.Minute, 20*time.Millisecond, time.Minute)

	done := make(chan struct{})
	go func() {
		runner.handle(context.Background(), msg.data, msg)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler hung")
	}
	if send.calls != 1 {
		t.Fatalf("send calls=%d", send.calls)
	}
	if store.finished != "" {
		t.Fatalf("finished=%s", store.finished)
	}
}

func TestDuplicateDeliveryDoesNotDoubleSend(t *testing.T) {
	job := sampleJob()
	store := &fakeStore{found: true, claimed: true, job: job}
	send := &blockingSender{}
	msg := &fakeMsg{data: []byte(job.ID.String())}
	runner := testRunner(store, send)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runner.handle(ctx, msg.data, msg)
		}()
	}
	time.Sleep(30 * time.Millisecond)
	cancel()
	wg.Wait()
	if send.calls != 1 {
		t.Fatalf("send calls=%d", send.calls)
	}
}

func TestSendTimeoutMarksUnknown(t *testing.T) {
	job := sampleJob()
	leaseEnd := time.Now().UTC().Add(time.Minute)
	job.LeaseUntil = &leaseEnd
	store := &fakeStore{found: true, claimed: true, job: job}
	send := &blockingSender{}
	msg := &fakeMsg{data: []byte(job.ID.String())}
	runner := testRunner(store, send)
	runner.SetLeasePolicy(time.Minute, time.Minute, 80*time.Millisecond)

	runner.handle(context.Background(), msg.data, msg)
	if send.calls != 1 {
		t.Fatalf("send calls=%d", send.calls)
	}
	if store.finished != domain.StatusUnknown {
		t.Fatalf("finished=%s", store.finished)
	}
	if store.last == nil || *store.last != domain.LastErrorSendTimeout {
		t.Fatalf("last=%v", store.last)
	}
	if msg.acks != 1 {
		t.Fatalf("acks=%d", msg.acks)
	}
}

func TestHandleSendInterruptedByShutdown(t *testing.T) {
	job := sampleJob()
	store := &fakeStore{found: true, claimed: true, job: job}
	send := &blockingSender{}
	msg := &fakeMsg{data: []byte(job.ID.String())}
	runner := testRunner(store, send)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runner.handle(ctx, msg.data, msg)
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler hung")
	}
	if send.calls != 1 {
		t.Fatalf("calls=%d", send.calls)
	}
	if store.finished != "" {
		t.Fatalf("finished=%s", store.finished)
	}
	if msg.acks != 0 {
		t.Fatalf("acks=%d", msg.acks)
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
	r := &Runner{
		log:         slog.New(slog.DiscardHandler),
		store:       store,
		sender:      send,
		workerID:    "worker-test",
		retryDelays: DBRetryDelays,
		nakDelay:    NakDelay,
	}
	r.SetLeasePolicy(DefaultLeaseDuration, DefaultLeaseRenewInterval, DefaultSendTimeout)
	return r
}

func sampleJob() domain.Job {
	attempt := uuid.New()
	owner := "worker-test"
	lease := time.Now().UTC().Add(time.Minute)
	return domain.Job{
		ID:            uuid.New(),
		RecipientID:   "user-1",
		Status:        domain.StatusQueued,
		Payload:       []byte(`{"to":"a","subject":"s","body":"b"}`),
		SendAttemptID: &attempt,
		WorkerID:      &owner,
		LeaseUntil:    &lease,
	}
}

func dialErr() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
}

type fakeStore struct {
	claimErr        error
	markErr         error
	renewErr        error
	spuriousMarkErr bool
	finishFails     int
	finishCalls     int
	job             domain.Job
	found           bool
	claimed         bool
	marked          bool
	finished        domain.Status
	last            *string
	renewHang       atomic.Bool
	renewedLease    time.Time
	renewCalls      atomic.Int32
}

func (f *fakeStore) RenewLease(ctx context.Context, _ uuid.UUID, _ uuid.UUID, _ string, lease time.Duration) (time.Time, error) {
	f.renewCalls.Add(1)
	if f.renewHang.Load() {
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-time.After(24 * time.Hour):
			return time.Time{}, context.DeadlineExceeded
		}
	}
	if f.renewErr != nil {
		return time.Time{}, f.renewErr
	}
	until := time.Now().UTC().Add(lease)
	f.renewedLease = until
	return until, nil
}

func (f *fakeStore) Claim(context.Context, uuid.UUID, string, time.Duration) (domain.Job, bool, bool, error) {
	if f.claimErr != nil {
		return domain.Job{}, false, false, f.claimErr
	}
	return f.job, f.found, f.claimed, nil
}

func (f *fakeStore) MarkSendStarted(_ context.Context, id, attempt uuid.UUID, _ string) (time.Time, error) {
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

func (f *fakeStore) FinishAttempt(_ context.Context, _ uuid.UUID, _ uuid.UUID, _ string, status domain.Status, lastError *string) error {
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

type blockingSender struct {
	calls int
}

func (b *blockingSender) Send(ctx context.Context, _ sender.Job) error {
	b.calls++
	<-ctx.Done()
	return ctx.Err()
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
