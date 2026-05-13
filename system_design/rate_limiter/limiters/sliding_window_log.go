package limiters

import (
	"context"
	"time"
)

type SlidingWindowLogLimiter struct {
	logSize    int
	windowSize time.Duration
	log        []time.Time
}

func NewSlidingWindowLogLimiter(logSize int, windowSize time.Duration) *SlidingWindowLogLimiter {
	return &SlidingWindowLogLimiter{
		logSize: logSize, windowSize: windowSize, log: make([]time.Time, 0),
	}
}

func (l *SlidingWindowLogLimiter) Start(ctx context.Context) {
	// no op
}

func (l *SlidingWindowLogLimiter) Acquire() bool {
	now := time.Now()
	start := now.Add(-l.windowSize)
	newLog := make([]time.Time, 0)

	// rebuild log
	for _, t := range l.log {
		if t.After(start) {
			newLog = append(newLog, t)
		}
	}

	if len(newLog) == l.logSize {
		return false
	}

	newLog = append(newLog, now)
	l.log = newLog

	return true
}
