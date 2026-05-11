package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.come/Amertz08/learning-go/system_design/url_shortener/internal"
	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/cache"
	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/server"
)

func main() {
	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	hasher := internal.NewFakeHasher()
	store := internal.NewFakeDataStore()
	c := cache.NewRedisCache("localhost", "6739")
	defer func() {
		if err := c.Close(); err != nil {
			logger.Error("error closing cache", slog.Any("error", err))
		}
	}()

	srv := server.NewServer(logger, hasher, store, c)

	go func() {
		logger.Info("starting server...")
		if err := srv.ListenAndServe(); err != nil {
			if errors.Is(err, http.ErrServerClosed) {
				logger.Info("server closed gracefully")
				return
			}
			logger.Error("error running server", slog.Any("error", err))
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-ctx.Done()

		shutDownCtx := context.Background()
		shutDownCtx, shutDownCnc := context.WithTimeout(ctx, 10*time.Second)
		defer shutDownCnc()
		if err := srv.Shutdown(shutDownCtx); err != nil {
			logger.Error("error shutting down server", slog.Any("error", err))
		}
	}()

	wg.Wait()
}
