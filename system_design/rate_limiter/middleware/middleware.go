package middleware

import (
	"context"
	"net/http"
)

type Limiter interface {
	Start(ctx context.Context)
	Acquire() bool
}

// TODO: I think the way this is implemented unless you init a new
//
//	middleware for each handler you will be sharing a bucket.
type RateLimiterMiddleware struct {
	limiter Limiter
}

func (m *RateLimiterMiddleware) Wrap(next http.Handler) http.Handler {
	m.limiter.Start(context.Background())
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m.limiter.Acquire() {
			next.ServeHTTP(w, r)
		} else {
			w.WriteHeader(http.StatusTooManyRequests)
		}
	})
}
