package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"

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

	srv := server.NewServer(logger, h, s, c)
	if err = srv.Start(":8080"); err != nil {
		logger.Error("error shutting down server", slog.Any("error", err))
	}
}
