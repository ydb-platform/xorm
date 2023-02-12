package retry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultBackoff(t *testing.T) {
	bf := DefaultBackoff()
	for i := 0; i < 10; i++ {
		d := bf.Delay(i)
		n := time.Now()
		start := n.Add(bf.min)
		end := n.Add(bf.max)
		cur := n.Add(d)
		assert.WithinRange(t, cur, start, end)
	}
}

func TestBackoff(t *testing.T) {
	bf := DefaultBackoff()
	for i := 0; i < 10; i++ {
		d := bf.Delay(i)
		n := time.Now()
		start := n.Add(bf.min)
		end := n.Add(bf.max)
		cur := n.Add(d)
		assert.WithinRange(t, cur, start, end)
	}

	for _, v := range []struct {
		min      time.Duration
		max      time.Duration
		jitter   bool
		attempts int
	}{
		{
			min:      5 * time.Microsecond,
			max:      10 * time.Microsecond,
			jitter:   true,
			attempts: 0,
		},
		{
			min:      10 * time.Millisecond,
			max:      20 * time.Millisecond,
			jitter:   false,
			attempts: 1,
		},
		{
			min:      20 * time.Microsecond,
			max:      30 * time.Millisecond,
			jitter:   false,
			attempts: 2,
		},
		{
			min:      30 * time.Second,
			max:      40 * time.Second,
			jitter:   true,
			attempts: 70,
		},
		{
			min:      10 * time.Millisecond,
			max:      20 * time.Second,
			jitter:   true,
			attempts: 10,
		},
		{
			min:      1 * time.Second,
			max:      2 * time.Second,
			jitter:   false,
			attempts: 30,
		},
	} {
		bf := NewBackoff(v.min, v.max, v.jitter)
		d := bf.Delay(v.attempts)
		n := time.Now()
		start := n.Add(bf.min)
		end := n.Add(bf.max)
		cur := n.Add(d)
		assert.WithinRange(t, cur, start, end)
	}
}
