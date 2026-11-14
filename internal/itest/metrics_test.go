//go:build integration

package itest

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMetricsSnapshotReflectsJobs(t *testing.T) {
	at := time.Now().Add(time.Hour).UTC()
	_, _, err := env.post(jobBody("met-"+uuid.NewString(), "solo", at, "solo"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	counts, err := env.direct.MetricsSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts.ByStatus["scheduled"] < 1 {
		t.Fatalf("%v", counts)
	}
}
