package domain

// RecoveryStats counts lease recovery outcomes after a successful batch commit.
type RecoveryStats struct {
	Requeued       int
	Unknown        int
	ClaimExhausted int
}
