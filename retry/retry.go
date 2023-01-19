// reference: https://github.com/ydb-platform/ydb-go-sdk/blob/master/retry/retry.go
package retry

import (
	"context"
	"fmt"
)

type retryOptions struct {
	id         string
	idempotent bool
	backoff    BackoffInterface // default implement 'Decorrelated Jitter' algorithm
}

// !datbeohbbh! This function can be dialect.IsRetryable(err)
// or your custom function that check if an error can be retried
type checkRetryable func(error) bool

type retryOperation func(context.Context) error

type RetryOption func(*retryOptions)

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

// !datbeohbbh! Retry provide the best effort fo retrying operation
//
// Retry implements internal busy loop until one of the following conditions is met:
// - context was canceled or deadlined
// - retry operation returned nil as error
//
// Warning: if deadline without deadline or cancellation func Retry will be worked infinite
func Retry(ctx context.Context, check checkRetryable, f retryOperation, opts ...RetryOption) error {
	options := &retryOptions{
		backoff: DefaultBackoff(),
	}
	for _, o := range opts {
		if o != nil {
			o(options)
		}
	}

	attemps := 0
	for {
		attemps++
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
				return fmt.Errorf("error is not retryable. Retry process with id '%s': %v",
					options.id, err)
			}
			if !options.idempotent {
				return fmt.Errorf("operation is not idempotent. Retry process with id '%s': %v",
					options.id, err)
			}
			if err = wait(ctx, options.backoff, attemps); err != nil {
				return fmt.Errorf("error in retry process with id '%s': %v", options.id, err)
			}
		}
	}
}

func wait(ctx context.Context, backoff BackoffInterface, attemps int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-backoff.Wait(attemps):
		return nil
	}
}
