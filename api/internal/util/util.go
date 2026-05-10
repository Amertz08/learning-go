package util

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
)

// InitLogger initializes a [slog.Logger]
func InitLogger(level slog.Level) *slog.Logger {
	opts := slog.HandlerOptions{
		Level: level,
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &opts))
	slog.SetDefault(logger)
	return logger
}

// InitContext creates a context and cancel function that listens for an interrupt signal
func InitContext() (context.Context, context.CancelFunc) {
	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	return ctx, cancel
}
