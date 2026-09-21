package main

import (
	"context"
	"dispatch.local/dispatch/internal/store"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "dispatch-server:", err)
		os.Exit(2)
	}
}

func openPool(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := os.Getenv("DISPATCH_DATABASE_URL")
	if dsn == "" {
		return nil, errors.New("DISPATCH_DATABASE_URL is required")
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid database configuration")
	}
	config.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, errors.New("database initialization failed")
	}
	check, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if pool.Ping(check) != nil {
		pool.Close()
		return nil, errors.New("database connection failed")
	}
	return pool, nil
}

func run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 1 && args[0] == "--version" {
		_, err := fmt.Fprintln(out, "dispatch-server 0.1.0-dev")
		return err
	}
	if len(args) == 0 {
		return errors.New("usage: dispatch-server serve|migrate|project create|token create|token revoke")
	}
	if args[0] == "serve" {
		c, err := parseServeConfig(args[1:])
		if err != nil {
			return err
		}
		pool, err := openPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()
		if err := store.Migrate(ctx, pool); err != nil {
			return fmt.Errorf("schema compatibility check: %w", err)
		}
		return serve(ctx, pool, c, out)
	}
	return operator(ctx, args, out)
}
