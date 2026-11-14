package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/VitaliiKarakay/goit-capstone/migrations"
)

type Store struct {
	pool *pgxpool.Pool
}

func Connect(ctx context.Context, url string, log *slog.Logger) (*Store, error) {
	delay := 200 * time.Millisecond
	var last error
	for {
		pool, err := pgxpool.New(ctx, url)
		if err == nil {
			err = pool.Ping(ctx)
		}
		if err == nil {
			return &Store{pool: pool}, nil
		}
		if pool != nil {
			pool.Close()
		}
		last = err
		if log != nil {
			log.Error("database connect", "err", err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			if last != nil {
				return nil, last
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
		if delay < 2*time.Second {
			delay *= 2
		}
	}
}

func (s *Store) Close() {
	s.pool.Close()
}

func Migrate(ctx context.Context, url string) error {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return err
	}
	defer db.Close()

	delay := 200 * time.Millisecond
	var last error
	for {
		last = db.PingContext(ctx)
		if last == nil {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("database ping: %w", last)
		case <-timer.C:
		}
		if delay < 2*time.Second {
			delay *= 2
		}
	}

	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
}
