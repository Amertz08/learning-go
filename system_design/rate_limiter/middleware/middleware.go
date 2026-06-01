package middleware

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v5"
)

type Limiter interface {
	Start(ctx context.Context)
	Acquire() bool
}

// LimiterCreationFunc allows the middleware to create a limiter per handler
type LimiterCreationFunc func() Limiter

type RateLimiterMiddleware struct {
	limiterCreationFunc LimiterCreationFunc
}

func NewRateLimiterMiddleware(limiterFunc LimiterCreationFunc) *RateLimiterMiddleware {
	return &RateLimiterMiddleware{limiterFunc}
}

func (m *RateLimiterMiddleware) Wrap(next echo.HandlerFunc) echo.HandlerFunc {
	limiter := m.limiterCreationFunc()
	limiter.Start(context.Background()) // TODO: not sure about this context
	return func(c *echo.Context) error {
		if limiter.Acquire() {
			return next(c)
		} else {
			return echo.NewHTTPError(http.StatusTooManyRequests, "too many requests")
		}
	}
}
