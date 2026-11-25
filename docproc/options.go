package docproc

import (
	"log/slog"
	"math/rand/v2"
	"time"
)

const (
	defaultSubmitTimeout  = 30 * time.Second
	defaultLeaseTimeout   = 2 * time.Minute
	defaultPersistTimeout = 5 * time.Second
)

type Option func(*Service)

// WithClock replaces the time source. Tests use it to exercise backoff and
// leases without waiting.
func WithClock(now func() time.Time) Option {
	return func(s *Service) { s.now = now }
}

func WithLogger(l *slog.Logger) Option {
	return func(s *Service) { s.log = l }
}

// WithSubmitTimeout bounds a single request to the Principal.
// 0 means no extra limit (the caller's ctx still applies).
func WithSubmitTimeout(d time.Duration) Option {
	return func(s *Service) { s.submitTimeout = d }
}

// WithLeaseTimeout sets how long a PROCESSING document that was never
// recorded as submitted may sit before RecoverStale considers it stuck.
func WithLeaseTimeout(d time.Duration) Option {
	return func(s *Service) { s.leaseTimeout = d }
}

func WithBackoff(b Backoff) Option {
	return func(s *Service) { s.backoff = b }
}

// ExponentialBackoff: base * 2^(attempt-1), capped at max, with "equal
// jitter" (half fixed, half random). Jitter keeps thousands of documents that
// failed together from being retried in the very same second.
func ExponentialBackoff(base, max time.Duration) Backoff {
	if base <= 0 || max <= 0 {
		return nil // nonsensical configuration: treat it as no backoff
	}
	return func(attempt int) time.Duration {
		if attempt < 1 {
			attempt = 1
		}
		d := base << (attempt - 1)
		if d <= 0 || d > max { // d <= 0 catches overflow
			d = max
		}
		half := d / 2
		return half + rand.N(half+1)
	}
}
