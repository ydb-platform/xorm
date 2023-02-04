package ydb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAutoIncr(t *testing.T) {
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	type AutoIncr struct {
		ID   uint64 `xorm:"pk autoincr"`
		Data string `xorm:"data"`
	}

	assert.NoError(t, PrepareScheme(&AutoIncr{}))

	for i := 0; i < 10; i++ {
		l, r := i*100, (i+1)*100
		t.Run(fmt.Sprintf("test-autoincr-%d", i), func(t *testing.T) {
			for j := l; j < r; j++ {
				_, err := engine.Insert(&AutoIncr{
					Data: fmt.Sprintf("data - %d", j),
				})
				assert.NoError(t, err)
			}
		})
	}
}
