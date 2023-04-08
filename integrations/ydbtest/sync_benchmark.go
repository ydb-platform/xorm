package ydb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkSync(b *testing.B) {
	assert.NoError(b, PrepareScheme(&Users{}))
	assert.NoError(b, PrepareScheme(&Series{}))
	assert.NoError(b, PrepareScheme(&Seasons{}))
	assert.NoError(b, PrepareScheme(&Episodes{}))

	engine, err := enginePool.GetSchemeQueryEngine()
	assert.NoError(b, err)
	assert.NotNil(b, engine)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		assert.NoError(b, engine.Sync(
			&Users{},
			&Series{},
			&Seasons{},
			&Episodes{},
		))
	}
}
