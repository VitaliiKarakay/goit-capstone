package config

import (
	"testing"
	"time"
)

func TestLoadWorkerRejectsRenewIntervalTooLarge(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("WORKER_LEASE_DURATION", "10s")
	t.Setenv("WORKER_LEASE_RENEW_INTERVAL", "10s")
	if _, err := LoadWorker(); err == nil {
		t.Fatal("expected error")
	}
}

func TestLoadWorkerAcceptsSendTimeoutLongerThanLease(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("WORKER_LEASE_DURATION", "10s")
	t.Setenv("WORKER_LEASE_RENEW_INTERVAL", "3s")
	t.Setenv("WORKER_SEND_TIMEOUT", "90s")
	cfg, err := LoadWorker()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SendTimeout != 90*time.Second {
		t.Fatalf("send timeout=%s", cfg.SendTimeout)
	}
}
