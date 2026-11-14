package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
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
}

type Worker struct {
	DatabaseURL string
	NATSURL     string
	WorkerID    string
	LogLevel    string
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
	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
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
