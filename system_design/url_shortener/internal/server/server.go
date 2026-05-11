package server

import (
	"log/slog"
	"net"
	"net/http"

	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/handlers"
)

func NewServer(
	logger *slog.Logger,
	host string,
	port string,
	hasher handlers.HashService,
	store handlers.HashDataStore,
	cache handlers.HashCacheStore,
) *http.Server {
	mux := &http.ServeMux{}

	shortHandler := handlers.ShortenHandler(logger, hasher, store, cache)
	visitHandler := handlers.VisitHandler(logger, store, cache)

	mux.HandleFunc("POST /shorten", shortHandler)
	mux.HandleFunc("GET /v/{short_hash}", visitHandler)

	server := &http.Server{
		Handler: mux,
		Addr:    net.JoinHostPort(host, port),
	}
	return server
}
