package ydb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"xorm.io/builder"
)

func TestIterate(t *testing.T) {
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)

	type UserIterate struct {
		Uuid  int64 `xorm:"pk"`
		IsMan bool
	}

	assert.NoError(t, engine.NewSession().DropTable(&UserIterate{}))

	assert.NoError(t, engine.Sync(new(UserIterate)))

	_, err = engine.Insert(&UserIterate{
		Uuid:  int64(1),
		IsMan: true,
	})
	assert.NoError(t, err)

	_, err = engine.Insert(&UserIterate{
		Uuid:  int64(2),
		IsMan: false,
	})
	assert.NoError(t, err)

	cnt := int64(0)
	err = engine.Iterate(new(UserIterate), func(i int, bean interface{}) error {
		user := bean.(*UserIterate)
		if cnt == int64(0) {
			assert.EqualValues(t, 1, user.Uuid)
			assert.EqualValues(t, true, user.IsMan)
		} else {
			assert.EqualValues(t, 2, user.Uuid)
			assert.EqualValues(t, false, user.IsMan)
		}
		cnt++
		return nil
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 2, cnt)
}

func TestBufferIterate(t *testing.T) {
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)

	type UserBufferIterate struct {
		Uuid  int64 `xorm:"pk"`
		IsMan bool
	}

	assert.NoError(t, engine.NewSession().DropTable(&UserBufferIterate{}))

	assert.NoError(t, engine.Sync(new(UserBufferIterate)))

	var size = 20
	for i := 0; i < size; i++ {
		_, err := engine.Insert(&UserBufferIterate{
			Uuid:  int64(i + 1),
			IsMan: true,
		})
		assert.NoError(t, err)
	}

	var cnt int64 = 0
	err = engine.BufferSize(9).Iterate(new(UserBufferIterate), func(i int, bean interface{}) error {
		user := bean.(*UserBufferIterate)
		assert.EqualValues(t, cnt+1, user.Uuid)
		assert.EqualValues(t, true, user.IsMan)
		cnt++
		return nil
	})
	assert.NoError(t, err)
	assert.EqualValues(t, size, cnt)

	cnt = int64(0)
	err = engine.Limit(20).BufferSize(9).Iterate(new(UserBufferIterate), func(i int, bean interface{}) error {
		user := bean.(*UserBufferIterate)
		assert.EqualValues(t, cnt+1, user.Uuid)
		assert.EqualValues(t, true, user.IsMan)
		cnt++
		return nil
	})
	assert.NoError(t, err)
	assert.EqualValues(t, size, cnt)

	cnt = int64(0)
	err = engine.Limit(7).BufferSize(9).Iterate(new(UserBufferIterate), func(i int, bean interface{}) error {
		user := bean.(*UserBufferIterate)
		assert.EqualValues(t, cnt+1, user.Uuid)
		assert.EqualValues(t, true, user.IsMan)
		cnt++
		return nil
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 7, cnt)

	cnt = int64(0)
	err = engine.Where(builder.Lte{"uuid": int64(10)}).BufferSize(2).Iterate(new(UserBufferIterate), func(i int, bean interface{}) error {
		user := bean.(*UserBufferIterate)
		assert.EqualValues(t, cnt+1, user.Uuid)
		assert.EqualValues(t, true, user.IsMan)
		cnt++
		return nil
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 10, cnt)
}
