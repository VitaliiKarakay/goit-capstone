package worker

import "time"

const (
	// MaxDeliver is the JetStream delivery cap for one message.
	// A database outage naks with NakDelay, so this budget is about
	// MaxDeliver * NakDelay (plus in-process waits), not 20 immediate retries.
	MaxDeliver = 20
	AckWait    = 30 * time.Second
	NakDelay   = 5 * time.Second
	// AckProgressInterval resets AckWait while a message is being processed.
	// It must stay below AckWait so the broker does not redeliver in-flight work.
	AckProgressInterval = 10 * time.Second
)

// DBRetryDelays are the waits between claim attempts inside a single delivery.
// A blip shorter than their sum is absorbed without consuming another delivery.
// The first attempt runs immediately; these values are the gaps after failures.
var DBRetryDelays = []time.Duration{
	200 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	2 * time.Second,
}
