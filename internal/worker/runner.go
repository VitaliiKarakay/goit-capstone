package worker

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
	"github.com/VitaliiKarakay/goit-capstone/internal/sender"
)

type Store interface {
	Claim(ctx context.Context, id uuid.UUID, workerID string, lease time.Duration) (domain.Job, bool, bool, error)
	RenewLease(ctx context.Context, id, attempt uuid.UUID, workerID string, lease time.Duration) (time.Time, error)
	MarkSendStarted(ctx context.Context, id, attempt uuid.UUID, workerID string) (time.Time, error)
	FinishAttempt(ctx context.Context, id, attempt uuid.UUID, workerID string, status domain.Status, lastError *string) error
}

type Stats struct {
	DelayedNak           atomic.Int64
	NotOwnerRejects      atomic.Int64
	CanceledTerminalAcks atomic.Int64
}

type Runner struct {
	log                      *slog.Logger
	store                    Store
	sender                   sender.Sender
	workerID                 string
	retryDelays              []time.Duration
	nakDelay                 time.Duration
	progressInterval         time.Duration
	leaseDuration            time.Duration
	leaseRenewEvery          time.Duration
	leaseRenewRequestTimeout time.Duration
	sendTimeout              time.Duration
	inflight                 sync.Map
	Stats                    Stats
}

func New(log *slog.Logger, store Store, send sender.Sender, workerID string) *Runner {
	return &Runner{
		log:                      log,
		store:                    store,
		sender:                   send,
		workerID:                 workerID,
		retryDelays:              DBRetryDelays,
		nakDelay:                 NakDelay,
		progressInterval:         AckProgressInterval,
		leaseDuration:            DefaultLeaseDuration,
		leaseRenewEvery:          DefaultLeaseRenewInterval,
		leaseRenewRequestTimeout: DefaultLeaseRenewRequestTimeout,
		sendTimeout:              DefaultSendTimeout,
	}
}

func (r *Runner) SetLeasePolicy(lease, renewEvery, sendTimeout time.Duration) {
	if lease > 0 {
		r.leaseDuration = lease
	}
	if renewEvery > 0 {
		r.leaseRenewEvery = renewEvery
	}
	if sendTimeout > 0 {
		r.sendTimeout = sendTimeout
	}
}

func (r *Runner) Run(stop, work context.Context, pull func() (*nats.Subscription, error)) error {
	sub, err := pull()
	if err != nil {
		return err
	}
	defer func() { _ = sub.Unsubscribe() }()

	for {
		msgs, err := sub.Fetch(1, nats.MaxWait(time.Second))
		if len(msgs) == 0 {
			if stop.Err() != nil {
				return nil
			}
			if err != nil && !errors.Is(err, nats.ErrTimeout) {
				r.log.Error("fetch", "err", err, "worker_id", r.workerID)
				select {
				case <-stop.Done():
					return nil
				case <-time.After(time.Second):
				}
				next, pullErr := pull()
				if pullErr != nil {
					r.log.Error("resubscribe", "err", pullErr, "worker_id", r.workerID)
					continue
				}
				_ = sub.Unsubscribe()
				sub = next
			}
			continue
		}
		for _, msg := range msgs {
			r.handle(work, msg.Data, msg)
		}
		if stop.Err() != nil {
			return nil
		}
	}
}

type ackMessage interface {
	Ack(...nats.AckOpt) error
	NakWithDelay(time.Duration, ...nats.AckOpt) error
	InProgress(...nats.AckOpt) error
}

func (r *Runner) handle(ctx context.Context, data []byte, msg ackMessage) {
	id, err := uuid.Parse(strings.TrimSpace(string(data)))
	if err != nil {
		r.log.Error("discard message", "err", err, "worker_id", r.workerID)
		_ = msg.Ack()
		return
	}

	var job domain.Job
	var found, claimed bool
	err = retryDB(ctx, r.retryDelays, func() error {
		var claimErr error
		job, found, claimed, claimErr = r.store.Claim(ctx, id, r.workerID, r.leaseDuration)
		return claimErr
	})
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		r.delay(msg, id, "claim", err)
		return
	}
	if !found {
		r.log.Error("job not found", "job_id", id.String(), "worker_id", r.workerID)
		_ = msg.Ack()
		return
	}
	if !claimed {
		r.followUp(ctx, msg, job)
		return
	}
	maybePauseAfterClaim(ctx)
	if !r.tryInflight(job.ID) {
		r.delay(msg, job.ID, "duplicate delivery", errors.New("local handler already active"))
		return
	}
	defer r.releaseInflight(job.ID)
	r.attempt(ctx, msg, job)
}

func (r *Runner) followUp(ctx context.Context, msg ackMessage, job domain.Job) {
	switch job.Status {
	case domain.StatusSucceeded, domain.StatusFailed, domain.StatusCanceled, domain.StatusUnknown:
		if job.Status == domain.StatusCanceled {
			r.Stats.CanceledTerminalAcks.Add(1)
		}
		r.log.Info("skip terminal duplicate", "job_id", job.ID.String(), "status", job.Status, "worker_id", r.workerID)
		_ = msg.Ack()
	case domain.StatusProcessing:
		if !r.ownsJob(job) {
			r.rejectNotOwner(msg, job.ID, "resume processing")
			return
		}
		if r.leaseExpired(job) {
			r.delay(msg, job.ID, "lease expired", errors.New("processing lease expired"))
			return
		}
		if job.SendStartedAt != nil {
			r.delay(msg, job.ID, "send already started", errors.New("cannot resume send on redelivery"))
			return
		}
		if job.SendAttemptID == nil {
			r.delay(msg, job.ID, "resume processing", errors.New("missing send attempt id"))
			return
		}
		if !r.tryInflight(job.ID) {
			r.delay(msg, job.ID, "duplicate delivery", errors.New("local handler already active"))
			return
		}
		defer r.releaseInflight(job.ID)
		r.attempt(ctx, msg, job)
	case domain.StatusQueued:
		r.delay(msg, job.ID, "stale delivery", errors.New("job queued; wait for recovery publish"))
	default:
		r.log.Info("skip after status change", "job_id", job.ID.String(), "status", job.Status, "worker_id", r.workerID)
		_ = msg.Ack()
	}
}

func (r *Runner) attempt(ctx context.Context, msg ackMessage, job domain.Job) {
	if job.SendAttemptID == nil {
		r.log.Error("missing send attempt", "job_id", job.ID.String(), "worker_id", r.workerID)
		_ = msg.Ack()
		return
	}
	if !r.ownsJob(job) {
		r.rejectNotOwner(msg, job.ID, "attempt")
		return
	}

	attempt := *job.SendAttemptID
	id := job.ID

	attemptCtx, cancelAttempt := context.WithCancel(ctx)
	defer cancelAttempt()

	stopProgress := r.keepInProgress(ctx, msg)
	defer stopProgress()

	confirmedLease := r.confirmedLeaseUntil(job)
	stopLease := r.startLeaseHeartbeat(attemptCtx, cancelAttempt, id, attempt, confirmedLease)
	defer stopLease()

	var started time.Time
	err := retryDB(attemptCtx, r.retryDelays, func() error {
		var markErr error
		started, markErr = r.store.MarkSendStarted(attemptCtx, id, attempt, r.workerID)
		return markErr
	})
	if err != nil {
		if errors.Is(err, domain.ErrLeaseLost) {
			r.log.Info("mark send lost lease", "job_id", id.String(), "worker_id", r.workerID)
			return
		}
		if attemptCtx.Err() != nil {
			return
		}
		r.delay(msg, id, "mark send start", err)
		return
	}

	sendCtx, cancelSend := context.WithTimeout(attemptCtx, r.sendTimeout)
	defer cancelSend()

	sendErr := r.sender.Send(sendCtx, sender.Job{
		ID:            job.ID,
		RecipientID:   job.RecipientID,
		Payload:       job.Payload,
		ScheduledAt:   job.ScheduledAt,
		SendStartedAt: started,
	})
	if sendErr != nil {
		if attemptCtx.Err() != nil {
			r.log.Info("send interrupted by shutdown or lease loss", "job_id", id.String(), "worker_id", r.workerID, "err", sendErr)
			return
		}
		if errors.Is(sendErr, context.DeadlineExceeded) && sendCtx.Err() != nil {
			text := domain.LastErrorSendTimeout
			r.finishUntilDone(attemptCtx, msg, id, attempt, domain.StatusUnknown, &text)
			return
		}
		if errors.Is(sendErr, sender.ErrSimulatedSend) {
			text := sendErr.Error()
			r.finishUntilDone(attemptCtx, msg, id, attempt, domain.StatusFailed, &text)
			return
		}
		text := sendErr.Error()
		r.log.Error("sender failed", "job_id", id.String(), "worker_id", r.workerID, "err", sendErr)
		r.finishUntilDone(attemptCtx, msg, id, attempt, domain.StatusFailed, &text)
		return
	}
	r.finishUntilDone(attemptCtx, msg, id, attempt, domain.StatusSucceeded, nil)
}

func (r *Runner) confirmedLeaseUntil(job domain.Job) time.Time {
	if job.LeaseUntil != nil {
		return job.LeaseUntil.UTC()
	}
	return time.Now().UTC().Add(r.leaseDuration)
}

func (r *Runner) renewRequestTimeout() time.Duration {
	if r.leaseRenewRequestTimeout > 0 {
		return r.leaseRenewRequestTimeout
	}
	return DefaultLeaseRenewRequestTimeout
}

func (r *Runner) tryRenewLease(ctx context.Context, id, attempt uuid.UUID, confirmedUntil time.Time) (time.Time, error) {
	if !confirmedUntil.After(time.Now().UTC()) {
		return time.Time{}, domain.ErrLeaseLost
	}
	budgetCtx, cancelBudget := context.WithDeadline(ctx, confirmedUntil)
	defer cancelBudget()

	var renewed time.Time
	err := retryDB(budgetCtx, r.retryDelays, func() error {
		reqCtx, cancelReq := context.WithTimeout(budgetCtx, r.renewRequestTimeout())
		defer cancelReq()
		until, err := r.store.RenewLease(reqCtx, id, attempt, r.workerID, r.leaseDuration)
		if err != nil {
			return err
		}
		renewed = until.UTC()
		return nil
	})
	return renewed, err
}

func (r *Runner) startLeaseHeartbeat(ctx context.Context, cancelAttempt context.CancelFunc, id, attempt uuid.UUID, initialConfirmed time.Time) func() {
	interval := r.leaseRenewEvery
	if interval <= 0 {
		interval = DefaultLeaseRenewInterval
	}
	stop := make(chan struct{})
	var confirmedMu sync.Mutex
	confirmedUntil := initialConfirmed
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		resetLeaseTimer := func(t **time.Timer, until time.Time) {
			if !until.After(time.Now().UTC()) {
				return
			}
			d := time.Until(until)
			if *t == nil {
				*t = time.NewTimer(d)
				return
			}
			if !(*t).Stop() {
				select {
				case <-(*t).C:
				default:
				}
			}
			(*t).Reset(d)
		}

		if !confirmedUntil.After(time.Now().UTC()) {
			cancelAttempt()
			return
		}
		var leaseTimer *time.Timer
		resetLeaseTimer(&leaseTimer, confirmedUntil)
		defer func() {
			if leaseTimer != nil {
				leaseTimer.Stop()
			}
		}()

		for {
			var leaseC <-chan time.Time
			if leaseTimer != nil {
				leaseC = leaseTimer.C
			}
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-leaseC:
				cancelAttempt()
				return
			case <-ticker.C:
				confirmedMu.Lock()
				until := confirmedUntil
				confirmedMu.Unlock()
				if !until.After(time.Now().UTC()) {
					cancelAttempt()
					return
				}
				renewed, err := r.tryRenewLease(ctx, id, attempt, until)
				if err == nil {
					confirmedMu.Lock()
					confirmedUntil = renewed
					confirmedMu.Unlock()
					resetLeaseTimer(&leaseTimer, renewed)
					continue
				}
				if errors.Is(err, domain.ErrLeaseLost) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || ctx.Err() != nil {
					cancelAttempt()
					return
				}
				r.log.Error("lease renew", "job_id", id.String(), "worker_id", r.workerID, "err", err)
				cancelAttempt()
				return
			}
		}
	}()
	return func() { close(stop) }
}

func (r *Runner) keepInProgress(ctx context.Context, msg ackMessage) func() {
	interval := r.progressInterval
	if interval <= 0 {
		interval = AckProgressInterval
	}
	stop := make(chan struct{})
	go func() {
		_ = msg.InProgress()
		if interval <= 0 {
			return
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := msg.InProgress(); err != nil {
					r.log.Error("in progress", "err", err, "worker_id", r.workerID)
				}
			}
		}
	}()
	return func() { close(stop) }
}

func (r *Runner) finishUntilDone(ctx context.Context, msg ackMessage, id, attempt uuid.UUID, status domain.Status, lastError *string) {
	for {
		err := retryDB(ctx, r.retryDelays, func() error {
			return r.store.FinishAttempt(ctx, id, attempt, r.workerID, status, lastError)
		})
		if err == nil {
			_ = msg.Ack()
			return
		}
		if errors.Is(err, domain.ErrLeaseLost) || errors.Is(err, domain.ErrNotProcessing) {
			r.log.Info("result already recorded or lease lost", "job_id", id.String(), "worker_id", r.workerID)
			_ = msg.Ack()
			return
		}
		if ctx.Err() != nil {
			return
		}
		r.log.Error("finish failed", "job_id", id.String(), "worker_id", r.workerID, "err", err)
		if !wait(ctx, r.nakDelay) {
			return
		}
	}
}

func (r *Runner) tryInflight(id uuid.UUID) bool {
	_, loaded := r.inflight.LoadOrStore(id, struct{}{})
	return !loaded
}

func (r *Runner) releaseInflight(id uuid.UUID) {
	r.inflight.Delete(id)
}

func (r *Runner) leaseExpired(job domain.Job) bool {
	if job.LeaseUntil == nil {
		return true
	}
	return !job.LeaseUntil.After(time.Now().UTC())
}

func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (r *Runner) rejectNotOwner(msg ackMessage, id uuid.UUID, op string) {
	r.Stats.NotOwnerRejects.Add(1)
	r.delay(msg, id, op, errors.New("job owned by another worker"))
}

func (r *Runner) delay(msg ackMessage, id uuid.UUID, op string, err error) {
	r.log.Error(op+" failed", "job_id", id.String(), "worker_id", r.workerID, "err", err, "nak_delay", r.nakDelay.String())
	r.Stats.DelayedNak.Add(1)
	_ = msg.NakWithDelay(r.nakDelay)
}

func (r *Runner) ownsJob(job domain.Job) bool {
	return job.WorkerID != nil && *job.WorkerID == r.workerID
}

func (r *Runner) SetDelays(nakDelay time.Duration, dbRetries []time.Duration) {
	if nakDelay > 0 {
		r.nakDelay = nakDelay
	}
	if dbRetries != nil {
		r.retryDelays = dbRetries
	}
}

func (r *Runner) SetProgressInterval(interval time.Duration) {
	r.progressInterval = interval
}
