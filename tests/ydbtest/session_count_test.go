package ydb

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"xorm.io/builder"
)

func TestCount(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()

	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	cond := builder.Lt{"user_id": sql.NullInt64{Int64: 5, Valid: true}}

	cnt, err := engine.
		Where(cond).
		Count(&Users{})
	assert.NoError(t, err)
	assert.EqualValues(t, 5, cnt)

	cnt, err = engine.Where(cond).Table("users").Count()
	assert.NoError(t, err)
	assert.EqualValues(t, 5, cnt)

	cnt, err = engine.Table("users").Count()
	assert.NoError(t, err)
	assert.EqualValues(t, len(usersData), cnt)
}

func TestSQLCount(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()

	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	sql := "SELECT COUNT(`user_id`) FROM `users`"
	cnt, err := engine.SQL(sql).Count()
	assert.NoError(t, err)
	assert.EqualValues(t, len(usersData), cnt)
}

func TestCountWithTableName(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()

	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	cnt, err := engine.Count(new(Users))
	assert.NoError(t, err)
	assert.EqualValues(t, len(usersData), cnt)

	cnt, err = engine.Count(Users{})
	assert.NoError(t, err)
	assert.EqualValues(t, len(usersData), cnt)
}

func TestCountWithSelectCols(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()

	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	cnt, err := engine.Cols("user_id").Count(new(Users))
	assert.NoError(t, err)
	assert.EqualValues(t, len(usersData), cnt)

	cnt, err = engine.Select("COUNT(`user_id`)").Count(Users{})
	assert.NoError(t, err)
	assert.EqualValues(t, len(usersData), cnt)
}

func TestCountWithGroupBy(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()
	for w, g := len(usersData)/4, 0; g < 4; g++ {
		for i := 0; i < w; i++ {
			usersData[w*g+i].Age = uint32(22 + g)
		}
	}

	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	cnt, err := engine.Cols("age").GroupBy("age").Count(&Users{})
	assert.NoError(t, err)
	assert.EqualValues(t, 4, cnt)

	cnt, err = engine.Select("COUNT(`age`)").GroupBy("age").Count(Users{})
	assert.NoError(t, err)
	assert.EqualValues(t, 4, cnt)
}
