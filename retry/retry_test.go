package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSetRetryOptions(t *testing.T) {
	opts := []RetryOption{
		WithMaxRetries(10),
		WithID("ut-test-retry"),
		WithIdempotent(true),
		WithBackoff(DefaultBackoff()),
	}

	rt := &retryOptions{
		ctx: context.Background(),
	}
	for _, o := range opts {
		if o != nil {
			o(rt)
		}
	}

	val, ok := rt.ctx.Value(maxRetriesKey{}).(int)
	assert.True(t, ok)
	assert.EqualValues(t, 10, val)

	assert.Equal(t, "ut-test-retry", rt.id)

	assert.True(t, rt.idempotent)

	assert.EqualValues(t, DefaultBackoff(), rt.backoff)
}

func TestMaxRetries(t *testing.T) {
	const mxRetries int = 10

	opts := []RetryOption{
		WithMaxRetries(mxRetries),
	}

	rt := &retryOptions{
		ctx: context.Background(),
	}
	for _, o := range opts {
		if o != nil {
			o(rt)
		}
	}

	val, ok := rt.ctx.Value(maxRetriesKey{}).(int)
	assert.True(t, ok)
	assert.EqualValues(t, mxRetries, val)

	for i := 0; i < mxRetries; i++ {
		assert.False(t, rt.reachMaxRetries(i))
	}

	assert.True(t, rt.reachMaxRetries(mxRetries+1))
}

func TestRetryTimeOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	err := Retry(ctx, func(err error) bool {
		return true
	}, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
			return nil
		}
	}, WithIdempotent(true))

	assert.True(t, errors.Is(err, context.DeadlineExceeded))
}

func TestRetryMaxRetriesExceeded(t *testing.T) {
	ctx := context.Background()

	utErr := errors.New("ut-error")

	err := Retry(ctx, func(err error) bool {
		return true
	}, func(ctx context.Context) error {
		return utErr
	},
		WithMaxRetries(10),
		WithIdempotent(true),
		WithBackoff(NewBackoff(1*time.Millisecond, 2*time.Millisecond, true)))

	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrMaxRetriesLimitExceed))
}

func TestRetryNonRetryable(t *testing.T) {
	ctx := context.Background()

	utErr := errors.New("ut-error")

	err := Retry(ctx, func(err error) bool {
		return false
	}, func(ctx context.Context) error {
		return utErr
	},
		WithBackoff(NewBackoff(1*time.Millisecond, 2*time.Millisecond, true)))

	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrNonRetryable))
}

func TestRetryIdempotent(t *testing.T) {
	ctx := context.Background()

	utErr := errors.New("ut-error")

	err := Retry(ctx, func(err error) bool {
		return true
	}, func(ctx context.Context) error {
		return utErr
	},
		WithIdempotent(false),
		WithBackoff(NewBackoff(1*time.Millisecond, 2*time.Millisecond, true)))

	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrNonIdempotent))
}

func TestRetryOk(t *testing.T) {
	const mxRetries int = 10
	ctx := context.Background()

	utErr := errors.New("ut-error")

	var c int = 0

	err := Retry(ctx, func(err error) bool {
		return true
	}, func(ctx context.Context) error {
		defer func() {
			c += 1
		}()
		if c == mxRetries {
			return nil
		}
		return utErr
	},
		WithMaxRetries(mxRetries),
		WithIdempotent(true),
		WithBackoff(NewBackoff(1*time.Millisecond, 2*time.Millisecond, true)))

	assert.NoError(t, err)
	assert.Greater(t, c, mxRetries)
}
