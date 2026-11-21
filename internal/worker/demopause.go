//go:build demorecovery

package worker

import (
	"context"
	"os"
	"time"
)

// maybePauseAfterClaim blocks until SIGKILL or context cancel when RECOVERY_DEMO_PAUSE_AFTER_CLAIM=1 (demo builds only).
func maybePauseAfterClaim(ctx context.Context) {
	if os.Getenv("RECOVERY_DEMO_PAUSE_AFTER_CLAIM") != "1" {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
