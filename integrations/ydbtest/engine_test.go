package ydb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPing(t *testing.T) {
	engine, err := enginePool.GetDefaultEngine()
	assert.NoError(t, err)

	assert.NoError(t, engine.Ping())
}

func TestPingContext(t *testing.T) {
	engine, err := enginePool.GetDefaultEngine()
	assert.NoError(t, err)

	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancelFunc()

	time.Sleep(time.Nanosecond)

	err = engine.PingContext(ctx)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "context deadline exceeded")
	}
}
