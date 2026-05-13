package funcs

import (
	"time"

	"github.come/Amertz08/learning-go/system_design/rate_limiter/limiters"
	"github.come/Amertz08/learning-go/system_design/rate_limiter/middleware"
)

func NewTokenLimiterClosure(count int) middleware.LimiterCreationFunc {
	limiter := limiters.NewTokenLimiter(count)
	return func() middleware.Limiter {
		return limiter
	}
}

func NewSlidingLimiterClosure(logSize int, windowSize time.Duration) middleware.LimiterCreationFunc {
	limiter := limiters.NewSlidingWindowLogLimiter(logSize, windowSize)
	return func() middleware.Limiter { return limiter }
}
