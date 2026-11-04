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
	Claim(ctx context.Context, id uuid.UUID, workerID string) (domain.Job, bool, bool, error)
	MarkSendStarted(ctx context.Context, id uuid.UUID) (time.Time, error)
	Finish(ctx context.Context, id uuid.UUID, status domain.Status, lastError *string) error
}

type Stats struct {
	DelayedNak atomic.Int64
}

type sendResult struct {
	status domain.Status
	last   *string
}

type Runner struct {
	log         *slog.Logger
	store       Store
	sender      sender.Sender
	workerID    string
	retryDelays []time.Duration
	nakDelay    time.Duration
	Stats       Stats

	mu      sync.Mutex
	results map[uuid.UUID]sendResult
}

func New(log *slog.Logger, store Store, send sender.Sender, workerID string) *Runner {
	return &Runner{
		log:         log,
		store:       store,
		sender:      send,
		workerID:    workerID,
		retryDelays: DBRetryDelays,
		nakDelay:    NakDelay,
		results:     map[uuid.UUID]sendResult{},
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
		r.dropResult(id)
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

// followUp continues a job that is already claimed. A missing send_started_at
// means the sender has not been called. A saved result means this process
// already called the sender and only the database write must be repeated.
// A result that exists only in a stopped process is left for a later iteration.
func (r *Runner) followUp(ctx context.Context, msg ackMessage, job domain.Job) {
	switch job.Status {
	case domain.StatusSucceeded, domain.StatusFailed:
		r.dropResult(job.ID)
		r.log.Info("skip terminal duplicate", "job_id", job.ID.String(), "status", job.Status, "worker_id", r.workerID)
		_ = msg.Ack()
	case domain.StatusProcessing:
		if job.SendStartedAt == nil {
			r.attempt(ctx, msg, job)
			return
		}
		status, last, ok := r.resultOf(job.ID)
		if !ok {
			r.delay(msg, job.ID, "resume finish", errors.New("send result is not on this worker"))
			return
		}
		r.persist(ctx, msg, job.ID, status, last)
	default:
		r.log.Error("skip unexpected status", "job_id", job.ID.String(), "status", job.Status, "worker_id", r.workerID)
		_ = msg.Ack()
	}
}

func (r *Runner) attempt(ctx context.Context, msg ackMessage, job domain.Job) {
	id := job.ID
	err := retryDB(ctx, r.retryDelays, func() error {
		_, markErr := r.store.MarkSendStarted(ctx, id)
		return markErr
	})
	if err != nil {
		if errors.Is(err, domain.ErrNotProcessing) {
			r.log.Info("send start already recorded", "job_id", id.String(), "worker_id", r.workerID)
			_ = msg.Ack()
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
	r.saveResult(id, status, lastError)
	r.persist(ctx, msg, id, status, lastError)
}

func (r *Runner) persist(ctx context.Context, msg ackMessage, id uuid.UUID, status domain.Status, lastError *string) {
	err := retryDB(ctx, r.retryDelays, func() error {
		return r.store.Finish(ctx, id, status, lastError)
	})
	if errors.Is(err, domain.ErrNotProcessing) {
		r.dropResult(id)
		r.log.Info("result already recorded", "job_id", id.String(), "worker_id", r.workerID)
		_ = msg.Ack()
		return
	}
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		r.saveResult(id, status, lastError)
		r.delay(msg, id, "finish", err)
		return
	}
	r.dropResult(id)
	_ = msg.Ack()
}

func (r *Runner) saveResult(id uuid.UUID, status domain.Status, last *string) {
	var copied *string
	if last != nil {
		text := *last
		copied = &text
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.results == nil {
		r.results = map[uuid.UUID]sendResult{}
	}
	r.results[id] = sendResult{status: status, last: copied}
}

func (r *Runner) resultOf(id uuid.UUID) (domain.Status, *string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	res, ok := r.results[id]
	return res.status, res.last, ok
}

func (r *Runner) dropResult(id uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.results, id)
}

func (r *Runner) delay(msg ackMessage, id uuid.UUID, op string, err error) {
	r.log.Error(op+" failed", "job_id", id.String(), "worker_id", r.workerID, "err", err, "nak_delay", r.nakDelay.String())
	r.Stats.DelayedNak.Add(1)
	_ = msg.NakWithDelay(r.nakDelay)
}
