package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"runlog/internal/runlog"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err := run(); err != nil {
		slog.Error("stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return errors.New("usage: runlog api|consumer|migrate|project-create NAME|project-revoke ID")
	}
	mode := os.Args[1]
	c := runlog.LoadConfig()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startup, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if mode == "consumer" {
		store, err := runlog.OpenMongo(startup, c)
		if err != nil {
			return err
		}
		defer disconnect(store)
		if err = store.Indexes(startup); err != nil {
			return err
		}
		k, err := runlog.Kafka(c, true)
		if err != nil {
			return err
		}
		defer k.Close()
		slog.Info("consumer started")
		return runlog.Consume(ctx, k, store)
	}
	p, err := runlog.OpenPostgres(startup, c)
	if err != nil {
		return err
	}
	defer p.Close()
	switch mode {
	case "migrate":
		return runlog.Migrate(startup, p)
	case "project-create":
		if len(os.Args) < 3 {
			return errors.New("project name required")
		}
		name := strings.TrimSpace(strings.Join(os.Args[2:], " "))
		if len(name) == 0 || len(name) > 128 {
			return errors.New("name must contain 1..128 bytes")
		}
		id, key, err := runlog.CreateProject(startup, p, name)
		if err != nil {
			return err
		}
		fmt.Printf("project_id=%s\napi_key=%s\n", id, key)
		return nil
	case "project-revoke":
		if len(os.Args) != 3 {
			return errors.New("project ID required")
		}
		return runlog.Revoke(startup, p, os.Args[2])
	case "api":
		store, err := runlog.OpenMongo(startup, c)
		if err != nil {
			return err
		}
		defer disconnect(store)
		k, err := runlog.Kafka(c, false)
		if err != nil {
			return err
		}
		defer k.Close()
		if err = k.Ping(startup); err != nil {
			return err
		}
		api := &runlog.API{PG: p, Store: store, Kafka: k}
		srv := &http.Server{
			Addr:              c.HTTP,
			Handler:           api.Handler(),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      15 * time.Second,
			IdleTimeout:       60 * time.Second,
		}
		done := make(chan error, 1)
		go func() {
			done <- srv.ListenAndServe()
		}()
		slog.Info("API started", "address", c.HTTP)
		select {
		case err := <-done:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-ctx.Done():
		}
		shutdown, finish := context.WithTimeout(context.Background(), 15*time.Second)
		defer finish()
		if err = srv.Shutdown(shutdown); err != nil {
			_ = srv.Close()
			return err
		}
		return nil
	default:
		return fmt.Errorf("unknown command %q", mode)
	}
}

func disconnect(s *runlog.Store) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.Client.Disconnect(ctx)
}
