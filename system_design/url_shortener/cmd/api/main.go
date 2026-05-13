package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/cache"
	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/config"
	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/database"
	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/hasher"
	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/server"
)

func main() {
	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg := config.ReadFromEnv()
	cfg.DatabaseConfig.Host = "localhost"
	cfg.DatabaseConfig.Port = "5432"
	cfg.DatabaseConfig.User = "postgres"
	cfg.DatabaseConfig.Password = "password"
	cfg.DatabaseConfig.Name = "postgres"
	cfg.RedisConfig.Host = "localhost"
	cfg.RedisConfig.Port = "6379"

	h := &hasher.Base64Hasher{}
	conn, err := pgx.Connect(ctx, cfg.DatabaseConfig.ConnString())
	if err != nil {
		logger.Error("could not connect to db", slog.Any("error", err))
		os.Exit(1)
	}
	s := database.NewPGDataStore(conn)
	c := cache.NewRedisCache(cfg.RedisConfig.Host, cfg.RedisConfig.Port)
	defer func() {
		if redisErr := c.Close(); redisErr != nil {
			logger.Error("error closing cache", slog.Any("error", err))
		}
	}()

	srv := server.NewServer(logger, "localhost", "8080", h, s, c)

	go func() {
		logger.Info(fmt.Sprintf("starting server -> %s", srv.Addr))
		if listenErr := srv.ListenAndServe(); listenErr != nil {
			if errors.Is(listenErr, http.ErrServerClosed) {
				logger.Info("server closed gracefully")
				return
			}
			logger.Error("error running server", slog.Any("error", listenErr))
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
		if shutDownErr := srv.Shutdown(shutDownCtx); shutDownErr != nil {
			logger.Error("error shutting down server", slog.Any("error", shutDownErr))
		}
	}()

	wg.Wait()
}
