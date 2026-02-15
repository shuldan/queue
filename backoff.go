package queue

import "time"

type BackoffStrategy interface {
	Delay(attempt int) time.Duration
}

type FixedBackoff struct {
	Duration time.Duration
}

func (f FixedBackoff) Delay(_ int) time.Duration {
	return f.Duration
}

type ExponentialBackoff struct {
	Base     time.Duration
	MaxDelay time.Duration
}

func (e ExponentialBackoff) Delay(attempt int) time.Duration {
	if attempt < 0 {
		return e.Base
	}

	const maxShift = 62
	if attempt > maxShift {
		return e.MaxDelay
	}

	delay := e.Base << uint(attempt)
	if delay > e.MaxDelay || delay < e.Base {
		return e.MaxDelay
	}

	return delay
}

// NoBackoff returns zero delay for all attempts.
type NoBackoff struct{}

// Delay always returns zero.
func (NoBackoff) Delay(_ int) time.Duration { return 0 }
