package worker

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
	"github.com/VitaliiKarakay/goit-capstone/internal/sender"
)

type Store interface {
	Claim(ctx context.Context, id uuid.UUID, workerID string) (domain.Job, bool, bool, error)
	MarkSendStarted(ctx context.Context, id, attempt uuid.UUID) (time.Time, error)
	Finish(ctx context.Context, id uuid.UUID, status domain.Status, lastError *string) error
}

type Stats struct {
	DelayedNak      atomic.Int64
	NotOwnerRejects atomic.Int64
}

type Runner struct {
	log              *slog.Logger
	store            Store
	sender           sender.Sender
	workerID         string
	retryDelays      []time.Duration
	nakDelay         time.Duration
	progressInterval time.Duration
	Stats            Stats
}

func New(log *slog.Logger, store Store, send sender.Sender, workerID string) *Runner {
	return &Runner{
		log:              log,
		store:            store,
		sender:           send,
		workerID:         workerID,
		retryDelays:      DBRetryDelays,
		nakDelay:         NakDelay,
		progressInterval: AckProgressInterval,
	}
}

// Run pulls from the shared durable consumer until stop is cancelled.
// work stays active for an in-flight message after stop, so a handler can finish.
// pull is called again after a fetch error so a dropped broker connection can recover.
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
		job, found, claimed, claimErr = r.store.Claim(ctx, id, r.workerID)
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
	r.attempt(ctx, msg, job)
}

// followUp continues processing for this delivery. send_started_at must stay empty
// until the sender runs, so a filled timestamp with no local send means another
// attempt owns the job or a previous process died after marking start.
func (r *Runner) followUp(ctx context.Context, msg ackMessage, job domain.Job) {
	switch job.Status {
	case domain.StatusSucceeded, domain.StatusFailed, domain.StatusCanceled:
		r.log.Info("skip terminal duplicate", "job_id", job.ID.String(), "status", job.Status, "worker_id", r.workerID)
		_ = msg.Ack()
	case domain.StatusProcessing:
		if job.SendStartedAt != nil {
			r.delay(msg, job.ID, "resume processing", errors.New("send already started on another attempt"))
			return
		}
		if job.SendAttemptID == nil {
			r.delay(msg, job.ID, "resume processing", errors.New("missing send attempt id"))
			return
		}
		if !r.ownsJob(job) {
			r.rejectNotOwner(msg, job.ID, "resume processing")
			return
		}
		r.attempt(ctx, msg, job)
	default:
		r.log.Error("skip unexpected status", "job_id", job.ID.String(), "status", job.Status, "worker_id", r.workerID)
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

	stopProgress := r.keepInProgress(ctx, msg)
	defer stopProgress()

	attempt := *job.SendAttemptID
	id := job.ID

	err := retryDB(ctx, r.retryDelays, func() error {
		_, markErr := r.store.MarkSendStarted(ctx, id, attempt)
		return markErr
	})
	if err != nil {
		if errors.Is(err, domain.ErrNotProcessing) {
			r.delay(msg, id, "mark send start", err)
			return
		}
		if ctx.Err() != nil {
			return
		}
		r.delay(msg, id, "mark send start", err)
		return
	}

	sendErr := r.sender.Send(ctx, sender.Job{ID: job.ID, RecipientID: job.RecipientID, Payload: job.Payload})
	status := domain.StatusSucceeded
	var lastError *string
	if sendErr != nil {
		status = domain.StatusFailed
		text := sendErr.Error()
		lastError = &text
		r.log.Error("sender failed", "job_id", id.String(), "worker_id", r.workerID, "err", sendErr)
	}
	r.finishUntilDone(ctx, msg, id, status, lastError)
}

// keepInProgress sends JetStream in-progress acks until stop is called or ctx ends.
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

// finishUntilDone keeps the JetStream message unacked and retries Finish with
// backoff until the database accepts the result or the work context ends.
func (r *Runner) finishUntilDone(ctx context.Context, msg ackMessage, id uuid.UUID, status domain.Status, lastError *string) {
	for {
		err := retryDB(ctx, r.retryDelays, func() error {
			return r.store.Finish(ctx, id, status, lastError)
		})
		if err == nil {
			_ = msg.Ack()
			return
		}
		if errors.Is(err, domain.ErrNotProcessing) {
			r.log.Info("result already recorded", "job_id", id.String(), "worker_id", r.workerID)
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

// SetDelays overrides retry pacing. Tests use shorter waits than production defaults.
func (r *Runner) SetDelays(nakDelay time.Duration, dbRetries []time.Duration) {
	if nakDelay > 0 {
		r.nakDelay = nakDelay
	}
	if dbRetries != nil {
		r.retryDelays = dbRetries
	}
}

// SetProgressInterval overrides how often in-progress acks are sent.
func (r *Runner) SetProgressInterval(interval time.Duration) {
	r.progressInterval = interval
}
