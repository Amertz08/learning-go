package middleware

import (
	"context"
	"net/http"
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

func (m *RateLimiterMiddleware) Wrap(next http.Handler) http.Handler {
	limiter := m.limiterCreationFunc()
	limiter.Start(context.Background()) // TODO: not sure about this context
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if limiter.Acquire() {
			next.ServeHTTP(w, r)
		} else {
			w.WriteHeader(http.StatusTooManyRequests)
		}
	})
}
