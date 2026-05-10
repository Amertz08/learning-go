package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.come/Amertz08/learning-go/api/internal/handlers"
	"github.come/Amertz08/learning-go/api/internal/middleware"
	"github.come/Amertz08/learning-go/api/internal/services"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, cancel := initContext()
	defer cancel()

	httpServer := newServer(logger, "localhost", "8080")

	// start the web server
	go func() {
		logger.Info("starting server")
		if err := httpServer.ListenAndServe(); err != nil {
			logger.Error("error running server", slog.Any("error", err))
		}
	}()

	gracefulShutDown(ctx, logger, httpServer)
}

// initContext creates a context and cancel function that listens for an interrupt signal
func initContext() (context.Context, context.CancelFunc) {
	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	return ctx, cancel
}

// newServer creates a new *[http.Server] to run
func newServer(logger *slog.Logger, host, port string) *http.Server {
	mux := http.NewServeMux()

	coolMiddleware := middleware.CoolMiddleware(logger)
	sumHandler := coolMiddleware(handlers.NewSumHandler(logger, services.Sum))

	mux.HandleFunc("GET /", handlers.IndexHandler(logger))
	mux.Handle("POST /sum", sumHandler)

	httpServer := &http.Server{
		Addr:    net.JoinHostPort(host, port),
		Handler: mux,
	}
	return httpServer
}

// gracefulShutDown handles graceful shutdown of the server
func gracefulShutDown(ctx context.Context, logger *slog.Logger, server *http.Server) {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		// block until main context ends
		<-ctx.Done()

		// create a shutdown context with a timeout to begin graceful shutdown
		shutdownCtx := context.Background()
		shutdownCtx, cancel := context.WithTimeout(shutdownCtx, 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("error shutting down http server:", slog.Any("error", err))
		}
	}()

	wg.Wait()
}
