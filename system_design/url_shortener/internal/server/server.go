package server

import (
	"log/slog"
	"time"

	"github.com/labstack/echo/v5"
	"github.come/Amertz08/learning-go/system_design/rate_limiter/middleware"
	"github.come/Amertz08/learning-go/system_design/rate_limiter/middleware/funcs"
)

func NewServer(
	logger *slog.Logger,
	hasher HashService,
	store HashDataStore,
	cache HashCacheStore,
) *echo.Echo {
	e := echo.New()
	e.Logger = logger

	//tokenLimiterFunc := funcs.NewTokenLimiterClosure(5)
	//slidingLimiterFunc := funcs.NewSlidingLimiterClosure(5, 20*time.Second)
	fixedWindowLimiter := funcs.NewFixedWindowLimiterClosure(5, 20*time.Second)
	rateLimiterMiddleware := middleware.NewRateLimiterMiddleware(fixedWindowLimiter)

	shortHandler := ShortenHandler(logger, hasher, store, cache)
	visitHandler := VisitHandler(logger, store, cache)

	e.POST("/shorten", rateLimiterMiddleware.Wrap(shortHandler))
	e.GET("/v/:short_hash", visitHandler)

	return e
}
