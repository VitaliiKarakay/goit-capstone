package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/VitaliiKarakay/goit-capstone/internal/config"
	natspub "github.com/VitaliiKarakay/goit-capstone/internal/messaging/nats"
	"github.com/VitaliiKarakay/goit-capstone/internal/sender"
	"github.com/VitaliiKarakay/goit-capstone/internal/storage/postgres"
	"github.com/VitaliiKarakay/goit-capstone/internal/worker"
)

func main() {
	log := newLogger(os.Getenv("LOG_LEVEL"))
	if err := run(log); err != nil {
		log.Error("exit", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.LoadWorker()
	if err != nil {
		return err
	}
	log = newLogger(cfg.LogLevel)

	stopCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	workCtx, cancelWork := context.WithCancel(context.Background())
	defer cancelWork()
	go func() {
		<-stopCtx.Done()
		timer := time.NewTimer(15 * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
			cancelWork()
		case <-workCtx.Done():
		}
	}()

	store, err := postgres.Connect(stopCtx, cfg.DatabaseURL, log)
	if err != nil {
		return err
	}
	defer store.Close()

	bus, err := natspub.Connect(stopCtx, cfg.NATSURL, "worker-"+cfg.WorkerID, log)
	if err != nil {
		return err
	}
	defer bus.Close()
	if err := bus.Ensure(stopCtx, worker.AckWait, worker.MaxDeliver); err != nil {
		return err
	}

	log.Info("worker started", "worker_id", cfg.WorkerID)
	runner := worker.New(log, store, sender.Mock{WorkerID: cfg.WorkerID, Logger: log}, cfg.WorkerID)
	return runner.Run(stopCtx, workCtx, bus.Pull)
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil || level == "" {
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
