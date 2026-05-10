package main

import (
	"errors"
	"log/slog"
	"net/http"

	"github.come/Amertz08/learning-go/api/internal/server"
	"github.come/Amertz08/learning-go/api/internal/store"
	"github.come/Amertz08/learning-go/api/internal/util"
)

func main() {
	logger := util.InitLogger(slog.LevelInfo)

	ctx, cancel := util.InitContext()
	defer cancel()

	userStore := store.NewInMemoryUserStore()

	httpServer := server.NewServer(logger, "localhost", "8080", userStore)

	// start the web server
	go func() {
		logger.Info("starting server")
		if err := httpServer.ListenAndServe(); err != nil {
			if errors.Is(err, http.ErrServerClosed) {
				logger.Info("server closed gracefully")
				return
			}
			logger.Error("error running server", slog.Any("error", err))
		}
	}()

	server.GracefulShutdown(ctx, logger, httpServer)
}
