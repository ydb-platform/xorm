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
	Min    time.Duration // default 5ms
	Max    time.Duration // default 15s
	Jitter bool          // default true
}

func DefaultBackoff() *Backoff {
	return &Backoff{
		Min:    5 * time.Millisecond,
		Max:    5 * time.Second,
		Jitter: true,
	}
}

func (b *Backoff) Wait(n int) <-chan time.Time {
	return time.After(b.Delay(n))
}

// Decorrelated Jitter
func (b *Backoff) Delay(i int) time.Duration {
	rand.Seed(time.Now().UnixNano())
	base := int64(b.Min)
	cap := int64(b.Max)

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

	if !b.Jitter {
		return time.Duration(rand.Int63n(bf + 1))
	}

	w := (bf >> 1) + rand.Int63n((bf>>1)+1)
	w = base + rand.Int63n(w*3-base+1)
	if w > cap {
		w = cap
	}

	return time.Duration(w)
}
