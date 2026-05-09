package middleware

import (
	"log/slog"
	"net/http"
)

type Middleware func(http.Handler) http.Handler

func CoolMiddleware(logger *slog.Logger) Middleware {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !isLoggedIn(r) {
				logger.Warn("user not logged in")
				http.NotFound(w, r)
				return
			}
			h.ServeHTTP(w, r)
		})
	}
}

func isLoggedIn(r *http.Request) bool {
	return true
}
