//go:build !demorecovery

package worker

import "context"

func maybePauseAfterClaim(context.Context) {}
