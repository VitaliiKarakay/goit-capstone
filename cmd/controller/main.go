package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/VitaliiKarakay/goit-capstone/internal/config"
	"github.com/VitaliiKarakay/goit-capstone/internal/controller"
	natspub "github.com/VitaliiKarakay/goit-capstone/internal/messaging/nats"
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
	cfg, err := config.LoadController()
	if err != nil {
		return err
	}
	log = newLogger(cfg.LogLevel)

	if len(os.Args) > 1 {
		if os.Args[1] != "migrate" {
			return errors.New("unknown command " + os.Args[1])
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := postgres.Migrate(ctx, cfg.DatabaseURL); err != nil {
			return err
		}
		log.Info("migrations applied")
		return nil
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := postgres.Connect(ctx, cfg.DatabaseURL, log)
	if err != nil {
		return err
	}
	defer store.Close()

	bus, err := natspub.Connect(ctx, cfg.NATSURL, "controller", log)
	if err != nil {
		return err
	}
	defer bus.Close()
	if err := bus.Ensure(ctx, worker.AckWait, worker.MaxDeliver); err != nil {
		return err
	}

	go controller.RunScheduler(ctx, log, store, cfg.SchedulerInterval, cfg.SchedulerBatch)
	go controller.RunOutbox(ctx, log, store, bus, cfg.OutboxInterval, cfg.OutboxBatch)

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           controller.NewAPI(log, store).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Info("controller listening", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	log.Info("controller stopping")
	return server.Shutdown(shutdownCtx)
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil || level == "" {
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
