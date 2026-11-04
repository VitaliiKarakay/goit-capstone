package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

const (
	Stream  = "JOBS"
	Subject = "jobs.dispatch"
	Durable = "workers"
)

type Bus struct {
	nc *nats.Conn
	js nats.JetStreamContext
}

func Connect(ctx context.Context, url, name string, log *slog.Logger) (*Bus, error) {
	opts := []nats.Option{
		nats.Name(name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(200 * time.Millisecond),
		nats.Timeout(2 * time.Second),
	}
	delay := 200 * time.Millisecond
	var last error
	for {
		nc, err := nats.Connect(url, opts...)
		if err == nil {
			js, jsErr := nc.JetStream()
			if jsErr == nil {
				return &Bus{nc: nc, js: js}, nil
			}
			nc.Close()
			err = jsErr
		}
		last = err
		if log != nil {
			log.Error("nats connect", "err", err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			if last != nil {
				return nil, last
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
		if delay < 2*time.Second {
			delay *= 2
		}
	}
}

func (b *Bus) Close() {
	if b.nc != nil {
		b.nc.Close()
	}
}

func (b *Bus) Ensure(ctx context.Context, ackWait time.Duration, maxDeliver int) error {
	if _, err := b.js.StreamInfo(Stream); err != nil {
		if !errors.Is(err, nats.ErrStreamNotFound) {
			return err
		}
		_, err = b.js.AddStream(&nats.StreamConfig{
			Name:       Stream,
			Subjects:   []string{Subject},
			Retention:  nats.WorkQueuePolicy,
			Storage:    nats.FileStorage,
			Duplicates: 2 * time.Minute,
			MaxAge:     24 * time.Hour,
		}, nats.Context(ctx))
		if err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
			return err
		}
	}
	if _, err := b.js.ConsumerInfo(Stream, Durable); err != nil {
		if !errors.Is(err, nats.ErrConsumerNotFound) {
			return err
		}
		_, err = b.js.AddConsumer(Stream, &nats.ConsumerConfig{
			Durable:       Durable,
			AckPolicy:     nats.AckExplicitPolicy,
			AckWait:       ackWait,
			MaxDeliver:    maxDeliver,
			MaxAckPending: 64,
			FilterSubject: Subject,
		}, nats.Context(ctx))
		if err != nil && !errors.Is(err, nats.ErrConsumerNameAlreadyInUse) {
			return err
		}
	}
	return nil
}

func (b *Bus) PublishJob(ctx context.Context, jobID uuid.UUID, messageID string) error {
	ack, err := b.js.Publish(Subject, []byte(jobID.String()), nats.MsgId(messageID), nats.Context(ctx))
	if err != nil {
		return err
	}
	if ack == nil || ack.Stream != Stream {
		return fmt.Errorf("jetstream publish was not acknowledged")
	}
	return nil
}

func (b *Bus) Pull() (*nats.Subscription, error) {
	return b.js.PullSubscribe(Subject, Durable, nats.Bind(Stream, Durable), nats.ManualAck())
}
