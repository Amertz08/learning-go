package server

import (
	"log/slog"
	"net"
	"net/http"
)

func NewServer(
	logger *slog.Logger,
	host string,
	port string,
	hasher HashService,
	store HashDataStore,
	cache HashCacheStore,
) *http.Server {
	mux := &http.ServeMux{}

	shortHandler := ShortenHandler(logger, hasher, store, cache)
	visitHandler := VisitHandler(logger, store, cache)

	mux.HandleFunc("POST /shorten", shortHandler)
	mux.HandleFunc("GET /v/{short_hash}", visitHandler)

	server := &http.Server{
		Handler: mux,
		Addr:    net.JoinHostPort(host, port),
	}
	return server
}
