// reference: https://github.com/ydb-platform/ydb-go-sdk/blob/master/retry/retry.go
package retry

import (
	"context"
	"errors"
	"fmt"
)

type retryOptions struct {
	id         string
	idempotent bool
	backoff    BackoffInterface // default implement 'Decorrelated Jitter' algorithm
	ctx        context.Context
}

var (
	ErrNonRetryable          = errors.New("retry error: non-retryable operation")
	ErrNonIdempotent         = errors.New("retry error: non-idempotent operation")
	ErrMaxRetriesLimitExceed = errors.New("retry error: max retries limit exceeded")
)

// !datbeohbbh! This function can be dialect.IsRetryable(err)
// or your custom function that check if an error can be retried
type checkRetryable func(error) bool

type retryOperation func(context.Context) error

type RetryOption func(*retryOptions)

type maxRetriesKey struct{}

func WithMaxRetries(maxRetriesValue int) RetryOption {
	return func(o *retryOptions) {
		o.ctx = context.WithValue(o.ctx, maxRetriesKey{}, maxRetriesValue)
	}
}

func WithID(id string) RetryOption {
	return func(o *retryOptions) {
		o.id = id
	}
}

func WithIdempotent(idempotent bool) RetryOption {
	return func(o *retryOptions) {
		o.idempotent = idempotent
	}
}

func WithBackoff(backoff BackoffInterface) RetryOption {
	return func(o *retryOptions) {
		o.backoff = backoff
	}
}

func (opts *retryOptions) reachMaxRetries(attempts int) bool {
	if mx, has := opts.ctx.Value(maxRetriesKey{}).(int); !has {
		return false
	} else {
		return attempts > mx
	}
}

// !datbeohbbh! Retry provide the best effort fo retrying operation
//
// Retry implements internal busy loop until one of the following conditions is met:
// - context was canceled or deadlined
// - retry operation returned nil as error
//
// Warning: if deadline without deadline or cancellation func Retry will be worked infinite
func Retry(ctx context.Context, check checkRetryable, f retryOperation, opts ...RetryOption) error {
	options := &retryOptions{
		ctx:     ctx,
		backoff: DefaultBackoff(),
	}
	for _, o := range opts {
		if o != nil {
			o(options)
		}
	}

	attempts := 0
	for !options.reachMaxRetries(attempts) {
		attempts++
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			err := f(ctx)
			if err == nil {
				return nil
			}
			canRetry := check(err)
			if !canRetry {
				return fmt.Errorf("Retry process with id '%s': %w",
					options.id, fmt.Errorf("%v: %w", err, ErrNonRetryable))
			}
			if !options.idempotent {
				return fmt.Errorf("Retry process with id '%s': %w",
					options.id, fmt.Errorf("%v: %w", err, ErrNonIdempotent))
			}
			if err = wait(ctx, options.backoff, attempts); err != nil {
				return fmt.Errorf("Retry process with id '%s': %w", options.id, err)
			}
		}
	}
	return fmt.Errorf("Retry process with id '%s': %w",
		options.id,
		fmt.Errorf("%v: %w",
			fmt.Errorf("max retries: %v", options.ctx.Value(maxRetriesKey{})),
			ErrMaxRetriesLimitExceed,
		))
}

func wait(ctx context.Context, backoff BackoffInterface, attempts int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-backoff.Wait(attempts):
		return nil
	}
}
