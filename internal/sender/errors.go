package sender

import "errors"

// ErrSimulatedSend is returned by the configurable mock when a send failure is simulated.
// The worker records a failed job status; this is not real message delivery.
var ErrSimulatedSend = errors.New("mock send failed")
