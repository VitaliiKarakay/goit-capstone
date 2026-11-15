//go:build demorecovery

package worker

import (
	"os"
	"time"
)

// maybePauseAfterClaim blocks until SIGKILL when RECOVERY_DEMO_PAUSE_AFTER_CLAIM=1 (demo builds only).
func maybePauseAfterClaim() {
	if os.Getenv("RECOVERY_DEMO_PAUSE_AFTER_CLAIM") != "1" {
		return
	}
	for {
		time.Sleep(time.Second)
	}
}
