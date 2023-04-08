package ydb

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestInt(t *testing.T) {
	type PR int
	type TestInt struct {
		Id   string `xorm:"pk VARCHAR"`
		Data PR
	}

	assert.NoError(t, PrepareScheme(&TestInt{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	_, err = engine.Insert(&TestInt{
		Id:   uuid.NewString(),
		Data: 1,
	})
	assert.NoError(t, err)

	var ret TestInt
	has, err := engine.Where("data = ?", PR(1)).Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)
}

func TestStringArray(t *testing.T) {
	type TestString struct {
		Id   string   `xorm:"pk VARCHAR"`
		Data []string `xorm:"TEXT"`
	}

	assert.NoError(t, PrepareScheme(&TestString{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	_, err = engine.Insert(&TestString{
		Id:   uuid.NewString(),
		Data: []string{"a", "b", "c"},
	})
	assert.NoError(t, err)

	var ret TestString
	has, err := engine.Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)

	assert.EqualValues(t, []string{"a", "b", "c"}, ret.Data)

	for i := 0; i < 10; i++ {
		_, err = engine.Insert(&TestString{
			Id:   uuid.NewString(),
			Data: []string{"a", "b", "c"},
		})
		assert.NoError(t, err)
	}

	var arr []TestString
	err = engine.Asc("id").Find(&arr)
	assert.NoError(t, err)
}

func TestMap(t *testing.T) {
	type TestMap struct {
		Id   string                 `xorm:"pk VARCHAR"`
		Data map[string]interface{} `xorm:"TEXT"`
	}

	assert.NoError(t, PrepareScheme(&TestMap{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	m := map[string]interface{}{
		"abc": "1",
		"xyz": "abc",
		"uvc": map[string]interface{}{
			"1": "abc",
			"2": "xyz",
		},
	}

	_, err = engine.Insert(&TestMap{
		Id:   uuid.NewString(),
		Data: m,
	})
	assert.NoError(t, err)

	var ret TestMap
	has, err := engine.Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)

	assert.EqualValues(t, m, ret.Data)
}
