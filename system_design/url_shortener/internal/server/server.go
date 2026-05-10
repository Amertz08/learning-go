package server

import (
	"log/slog"
	"net/http"

	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/handlers"
)

func NewServer(
	logger *slog.Logger,
	hasher handlers.HashService,
	store handlers.HashDataStore,
	cache handlers.HashCacheStore,
) *http.Server {
	mux := &http.ServeMux{}

	shortHandler := handlers.ShortenHandler(logger, hasher, store, cache)

	mux.HandleFunc("POST /shorten", shortHandler)

	server := &http.Server{
		Handler: mux,
	}
	return server
}
