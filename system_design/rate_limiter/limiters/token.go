package limiters

import (
	"context"
	"time"

	"github.come/Amertz08/learning-go/system_design/rate_limiter/middleware"
)

type tokens chan struct{}

type TokenLimiter struct {
	count  int
	tokens tokens
	ticker *time.Ticker
}

func NewTokenLimiter(count int) *TokenLimiter {
	// TODO: configurable timer
	toks := make(tokens, count)
	for i := 0; i < count; i++ {
		toks <- struct{}{}
	}

	return &TokenLimiter{
		count:  count,
		tokens: toks,
		ticker: time.NewTicker(2 * time.Second),
	}
}

// TODO: it seems weird to have the idea of 'middleware' here since
//
//	a token limiter does not have to be used in a middleware
func NewTokenLimiterClosure(count int) middleware.LimiterCreationFunc {
	limiter := NewTokenLimiter(count)
	return func() middleware.Limiter {
		return limiter
	}
}

// Start will kick off a goroutine to add tokens to the bucket
func (l *TokenLimiter) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-l.ticker.C:
				select {
				case l.tokens <- struct{}{}:
				default:
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Acquire returns true if a token can be retrieved from the bucket false otherwise
func (l *TokenLimiter) Acquire() bool {
	select {
	case <-l.tokens:
		return true
	default:
		return false
	}
}
