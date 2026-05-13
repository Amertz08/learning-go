package server

import (
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.come/Amertz08/learning-go/system_design/rate_limiter/middleware"
	"github.come/Amertz08/learning-go/system_design/rate_limiter/middleware/funcs"
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

	//tokenLimiterFunc := funcs.NewTokenLimiterClosure(5)
	slidinglimiterFunc := funcs.NewSlidingLimiterClosure(5, 20*time.Second)
	rateLimiterMiddleware := middleware.NewRateLimiterMiddleware(slidinglimiterFunc)

	shortHandler := ShortenHandler(logger, hasher, store, cache)
	visitHandler := VisitHandler(logger, store, cache)

	mux.HandleFunc("POST /shorten", rateLimiterMiddleware.Wrap(shortHandler))
	mux.HandleFunc("GET /v/{short_hash}", visitHandler)

	server := &http.Server{
		Handler: mux,
		Addr:    net.JoinHostPort(host, port),
	}
	return server
}
