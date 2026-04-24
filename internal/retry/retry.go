package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/basekick-labs/firn/internal/catalog"
)

// Config controls retry behaviour.
type Config struct {
	MaxAttempts int           // total attempts (not extra retries); must be >= 1
	BaseDelay   time.Duration // window for first backoff; 0 means no sleep
	MaxDelay    time.Duration // cap on backoff window; 0 means no cap
}

// Retryer executes a function with exponential backoff + full jitter.
type Retryer struct {
	cfg     Config
	sleepFn func(context.Context, time.Duration) error // swapped in tests
}

// New returns a Retryer configured from cfg.
func New(cfg Config) *Retryer {
	return &Retryer{cfg: cfg, sleepFn: contextSleep}
}

// Do calls fn until it returns nil, a non-retryable error, or MaxAttempts are
// exhausted. isRetryable decides whether an error should trigger another attempt.
// fn receives the zero-based attempt index. Backoff is applied between attempts.
func (r *Retryer) Do(ctx context.Context, isRetryable func(error) bool, fn func(attempt int) error) error {
	var lastErr error
	for attempt := 0; attempt < r.cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
			delay := r.backoff(attempt - 1)
			if delay > 0 {
				if err := r.sleepFn(ctx, delay); err != nil {
					return err
				}
			}
		}
		lastErr = fn(attempt)
		if lastErr == nil {
			return nil
		}
		if !isRetryable(lastErr) {
			return lastErr
		}
	}
	return fmt.Errorf("failed after %d attempts: %w", r.cfg.MaxAttempts, lastErr)
}

// backoff returns a random duration in [0, window) where window = min(BaseDelay*2^attempt, MaxDelay).
// attempt is 0-based and counts only the retries (first call has attempt=0).
// Overflow-safe: if the multiplication would exceed MaxDelay (or MaxInt64 when MaxDelay is zero),
// the window is clamped before the multiplication is attempted.
func (r *Retryer) backoff(attempt int) time.Duration {
	if r.cfg.BaseDelay <= 0 {
		return 0
	}
	cap := time.Duration(math.MaxInt64)
	if r.cfg.MaxDelay > 0 {
		cap = r.cfg.MaxDelay
	}
	// Determine the window without overflowing int64.
	// If BaseDelay already exceeds cap, just use cap.
	// Otherwise check whether shifting would overflow before doing it.
	shift := attempt
	if shift > 62 {
		shift = 62
	}
	var window time.Duration
	// BaseDelay * 2^shift overflows if BaseDelay > cap >> shift.
	if r.cfg.BaseDelay > cap>>uint(shift) {
		window = cap
	} else {
		window = r.cfg.BaseDelay * (1 << uint(shift))
		if window > cap {
			window = cap
		}
	}
	if window <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(window)))
}

// contextSleep sleeps for d, returning ctx.Err() if the context is cancelled first.
func contextSleep(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// IsConflict reports whether err is (or wraps) a catalog.ErrConflict.
func IsConflict(err error) bool {
	var c catalog.ErrConflict
	return errors.As(err, &c)
}
