package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
)

type Recovery struct {
	Requeued       prometheus.Counter
	Unknown        prometheus.Counter
	ClaimExhausted prometheus.Counter
}

func NewRecovery(reg prometheus.Registerer) *Recovery {
	r := &Recovery{
		Requeued: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_recovery_requeued_total",
			Help: "Jobs returned to queued after lease expiry without send_started_at.",
		}),
		Unknown: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_recovery_unknown_total",
			Help: "Jobs moved to unknown after lease expiry with send_started_at set.",
		}),
		ClaimExhausted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_recovery_claim_exhausted_total",
			Help: "Jobs marked failed after exceeding MAX_CLAIM_ATTEMPTS before send.",
		}),
	}
	reg.MustRegister(r.Requeued, r.Unknown, r.ClaimExhausted)
	return r
}

func (r *Recovery) Observe(stats domain.RecoveryStats) {
	if stats.Requeued > 0 {
		r.Requeued.Add(float64(stats.Requeued))
	}
	if stats.Unknown > 0 {
		r.Unknown.Add(float64(stats.Unknown))
	}
	if stats.ClaimExhausted > 0 {
		r.ClaimExhausted.Add(float64(stats.ClaimExhausted))
	}
}
