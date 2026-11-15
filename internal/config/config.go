package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/VitaliiKarakay/goit-capstone/internal/sender"
)

type Controller struct {
	HTTPAddr          string
	DatabaseURL       string
	NATSURL           string
	SchedulerInterval time.Duration
	SchedulerBatch    int
	OutboxInterval    time.Duration
	OutboxBatch       int
	LogLevel          string
	MetricsDBRefresh  time.Duration
	RecoveryInterval  time.Duration
	RecoveryBatch     int
	MaxClaimAttempts  int
}

type Worker struct {
	DatabaseURL     string
	NATSURL         string
	WorkerID        string
	LogLevel        string
	MetricsAddr     string
	Mock            sender.MockConfig
	LeaseDuration   time.Duration
	LeaseRenewEvery time.Duration
	SendTimeout     time.Duration
}

func LoadController() (Controller, error) {
	cfg := Controller{
		HTTPAddr:          getenv("HTTP_ADDR", "127.0.0.1:8080"),
		DatabaseURL:       os.Getenv("DATABASE_URL"),
		NATSURL:           getenv("NATS_URL", "nats://127.0.0.1:4222"),
		SchedulerInterval: time.Second,
		SchedulerBatch:    100,
		OutboxInterval:    500 * time.Millisecond,
		OutboxBatch:       50,
		LogLevel:          getenv("LOG_LEVEL", "info"),
	}
	if cfg.DatabaseURL == "" {
		return Controller{}, fmt.Errorf("DATABASE_URL is required")
	}
	var err error
	if cfg.SchedulerInterval, err = duration("SCHEDULER_INTERVAL", cfg.SchedulerInterval); err != nil {
		return Controller{}, err
	}
	if cfg.OutboxInterval, err = duration("OUTBOX_INTERVAL", cfg.OutboxInterval); err != nil {
		return Controller{}, err
	}
	if cfg.SchedulerBatch, err = positiveInt("SCHEDULER_BATCH", cfg.SchedulerBatch); err != nil {
		return Controller{}, err
	}
	if cfg.OutboxBatch, err = positiveInt("OUTBOX_BATCH", cfg.OutboxBatch); err != nil {
		return Controller{}, err
	}
	cfg.MetricsDBRefresh = 10 * time.Second
	if cfg.MetricsDBRefresh, err = nonNegativeDuration("METRICS_DB_REFRESH_INTERVAL", cfg.MetricsDBRefresh, true); err != nil {
		return Controller{}, err
	}
	cfg.RecoveryInterval = 5 * time.Second
	if cfg.RecoveryInterval, err = duration("RECOVERY_INTERVAL", cfg.RecoveryInterval); err != nil {
		return Controller{}, err
	}
	cfg.RecoveryBatch = 50
	if cfg.RecoveryBatch, err = positiveInt("RECOVERY_BATCH", cfg.RecoveryBatch); err != nil {
		return Controller{}, err
	}
	cfg.MaxClaimAttempts = 5
	if cfg.MaxClaimAttempts, err = positiveInt("MAX_CLAIM_ATTEMPTS", cfg.MaxClaimAttempts); err != nil {
		return Controller{}, err
	}
	return cfg, nil
}

func LoadWorker() (Worker, error) {
	cfg := Worker{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		NATSURL:     getenv("NATS_URL", "nats://127.0.0.1:4222"),
		WorkerID:    os.Getenv("WORKER_ID"),
		LogLevel:    getenv("LOG_LEVEL", "info"),
	}
	if cfg.DatabaseURL == "" {
		return Worker{}, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.WorkerID == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "worker"
		}
		cfg.WorkerID = host + "-" + strconv.Itoa(os.Getpid())
	}
	cfg.MetricsAddr = getenv("METRICS_ADDR", "0.0.0.0:9091")
	mock, err := sender.LoadMockConfig()
	if err != nil {
		return Worker{}, err
	}
	cfg.Mock = mock
	cfg.LeaseDuration = 45 * time.Second
	if cfg.LeaseDuration, err = duration("WORKER_LEASE_DURATION", cfg.LeaseDuration); err != nil {
		return Worker{}, err
	}
	cfg.LeaseRenewEvery = 10 * time.Second
	if cfg.LeaseRenewEvery, err = duration("WORKER_LEASE_RENEW_INTERVAL", cfg.LeaseRenewEvery); err != nil {
		return Worker{}, err
	}
	cfg.SendTimeout = 90 * time.Second
	if cfg.SendTimeout, err = duration("WORKER_SEND_TIMEOUT", cfg.SendTimeout); err != nil {
		return Worker{}, err
	}
	half := cfg.LeaseDuration / 2
	if cfg.LeaseRenewEvery >= half {
		return Worker{}, fmt.Errorf("WORKER_LEASE_RENEW_INTERVAL must be less than half of WORKER_LEASE_DURATION (%s)", half)
	}
	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func nonNegativeDuration(key string, fallback time.Duration, positiveDefault bool) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		if positiveDefault && fallback <= 0 {
			return 0, fmt.Errorf("%s must be a positive duration", key)
		}
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 || (positiveDefault && d <= 0) {
		return 0, fmt.Errorf("%s must be a non-negative duration", key)
	}
	return d, nil
}

func duration(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}
	return d, nil
}

func positiveInt(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return n, nil
}
