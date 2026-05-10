package server

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.come/Amertz08/learning-go/api/internal/handlers"
	"github.come/Amertz08/learning-go/api/internal/middleware"
	"github.come/Amertz08/learning-go/api/internal/services"
)

// NewServer creates a new *[http.Server] to run
func NewServer(logger *slog.Logger, host, port string, userStore handlers.UserStore) *http.Server {
	mux := http.NewServeMux()

	indexHandler := handlers.IndexHandler(logger)
	coolMiddleware := middleware.CoolMiddleware(logger)
	sumHandler := coolMiddleware(handlers.NewSumHandler(logger, services.Sum))
	userHandler := handlers.NewUserHandler(logger, userStore)

	mux.HandleFunc("GET /", indexHandler)
	mux.Handle("POST /sum", sumHandler)
	mux.Handle("POST /users", userHandler)

	httpServer := &http.Server{
		Addr:    net.JoinHostPort(host, port),
		Handler: mux,
	}
	return httpServer
}

// GracefulShutdown handles graceful shutdown of the server
func GracefulShutdown(ctx context.Context, logger *slog.Logger, server *http.Server) {
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
