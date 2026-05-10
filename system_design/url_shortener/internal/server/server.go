package server

import (
	"net/http"

	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/handlers"
)

func NewServer(
	hasher handlers.HashService,
	store handlers.HashDataStore,
	cache handlers.HashCacheStore,
) *http.Server {
	mux := &http.ServeMux{}

	shortHandler := handlers.ShortenHandler(hasher, store, cache)

	mux.HandleFunc("POST /shorten", shortHandler)

	server := &http.Server{
		Handler: mux,
	}
	return server
}
