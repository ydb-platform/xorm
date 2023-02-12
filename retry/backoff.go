// reference: https://aws.amazon.com/vi/blogs/architecture/exponential-backoff-and-jitter/
package retry

import (
	"math"
	"math/rand"
	"time"
)

type BackoffInterface interface {
	Wait(n int) <-chan time.Time

	Delay(i int) time.Duration
}

type Backoff struct {
	min    time.Duration // default 5ms
	max    time.Duration // default 5s
	jitter bool          // default true
}

func DefaultBackoff() *Backoff {
	return &Backoff{
		min:    5 * time.Millisecond,
		max:    5 * time.Second,
		jitter: true,
	}
}

func NewBackoff(min, max time.Duration, jitter bool) *Backoff {
	return &Backoff{
		min:    min,
		max:    max,
		jitter: jitter,
	}
}

func (b *Backoff) Wait(n int) <-chan time.Time {
	return time.After(b.Delay(n))
}

// Decorrelated Jitter
func (b *Backoff) Delay(i int) time.Duration {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	base := int64(b.min)
	cap := int64(b.max)

	if base >= cap {
		return time.Duration(cap)
	}

	t := int(math.Log2(float64(cap)/float64(base))) + 1
	if i > t {
		i = t
	}

	bf := base * int64(1<<i)
	if bf > cap {
		bf = cap
	}

	if !b.jitter {
		return time.Duration(bf)
	}

	w := (bf >> 1) + rand.Int63n((bf>>1)+1)
	w = base + rand.Int63n(w*3-base+1)
	if w > cap {
		w = cap
	}

	return time.Duration(w)
}
