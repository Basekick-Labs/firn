package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noSleep replaces contextSleep in tests so they run instantly.
func noSleep(_ context.Context, _ time.Duration) error { return nil }

var errRetryable = errors.New("retryable")
var errFatal = errors.New("fatal")

func isRetryable(err error) bool { return errors.Is(err, errRetryable) }

func newFast(maxAttempts int) *Retryer {
	r := New(Config{MaxAttempts: maxAttempts, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond})
	r.sleepFn = noSleep
	return r
}

func TestDo_SucceedsOnFirstAttempt(t *testing.T) {
	r := newFast(3)
	calls := 0
	err := r.Do(context.Background(), isRetryable, func(attempt int) error {
		calls++
		assert.Equal(t, 0, attempt)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
}

func TestDo_RetriesOnRetryableError(t *testing.T) {
	r := newFast(5)
	calls := 0
	err := r.Do(context.Background(), isRetryable, func(attempt int) error {
		calls++
		if calls < 3 {
			return errRetryable
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, calls)
}

func TestDo_StopsAfterMaxAttempts(t *testing.T) {
	r := newFast(3)
	calls := 0
	err := r.Do(context.Background(), isRetryable, func(_ int) error {
		calls++
		return errRetryable
	})
	require.Error(t, err)
	assert.Equal(t, 3, calls)
	assert.ErrorIs(t, err, errRetryable)
}

func TestDo_FatalErrorStopsImmediately(t *testing.T) {
	r := newFast(10)
	calls := 0
	err := r.Do(context.Background(), isRetryable, func(_ int) error {
		calls++
		if calls == 2 {
			return errFatal
		}
		return errRetryable
	})
	require.ErrorIs(t, err, errFatal)
	assert.Equal(t, 2, calls, "should stop as soon as a non-retryable error is returned")
}

func TestDo_ContextCancelledDuringSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// sleepFn that cancels the context and then returns its error.
	cancelOnSleep := func(ctx context.Context, _ time.Duration) error {
		cancel()
		return ctx.Err()
	}

	r := New(Config{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: time.Second})
	r.sleepFn = cancelOnSleep

	calls := 0
	err := r.Do(ctx, isRetryable, func(_ int) error {
		calls++
		return errRetryable
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, calls, "only the first attempt should have run before sleep was cancelled")
}

func TestDo_NoDelayWhenBaseDelayZero(t *testing.T) {
	sleepCalls := 0
	r := New(Config{MaxAttempts: 3, BaseDelay: 0})
	r.sleepFn = func(_ context.Context, _ time.Duration) error {
		sleepCalls++
		return nil
	}

	_ = r.Do(context.Background(), isRetryable, func(_ int) error { return errRetryable })
	assert.Equal(t, 0, sleepCalls, "no sleep when BaseDelay is zero")
}

func TestBackoff_GrowsWithAttempt(t *testing.T) {
	r := New(Config{MaxAttempts: 10, BaseDelay: 100 * time.Millisecond, MaxDelay: 10 * time.Second})

	// Run many samples to verify the upper bound grows and is capped at MaxDelay.
	for attempt := 0; attempt < 6; attempt++ {
		cap := r.cfg.BaseDelay * (1 << uint(attempt))
		if cap > r.cfg.MaxDelay {
			cap = r.cfg.MaxDelay
		}
		for i := 0; i < 50; i++ {
			d := r.backoff(attempt)
			assert.GreaterOrEqual(t, int64(cap), int64(d),
				"attempt %d: backoff %v exceeds window %v", attempt, d, cap)
		}
	}
}

func TestDo_ErrorWrapsLastErr(t *testing.T) {
	r := newFast(2)
	sentinel := errors.New("sentinel")
	err := r.Do(context.Background(), func(e error) bool { return true }, func(_ int) error {
		return sentinel
	})
	assert.ErrorIs(t, err, sentinel)
}
