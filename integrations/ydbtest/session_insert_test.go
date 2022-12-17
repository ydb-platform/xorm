package ydb

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInsertOne(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	user := Users{
		Name: "Dat",
		Age:  21,
		Account: Account{
			UserID: sql.NullInt64{Int64: 1234, Valid: true},
			Number: "56789",
		},
	}

	_, err = engine.InsertOne(&user)
	assert.NoError(t, err)

	has, err := engine.Exist(&user)
	assert.NoError(t, err)
	assert.True(t, has)
}

func TestInsertMultiStruct(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	users := getUsersData()

	_, err = engine.Insert(&users)
	assert.NoError(t, err)

	cnt, err := engine.Count(&Users{})
	assert.NoError(t, err)
	assert.Equal(t, int64(len(users)), cnt)
}

func TestInsertCreated(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	curTime := time.Now()
	users := getUsersData()

	_, err = engine.Insert(&users)
	assert.NoError(t, err)

	err = engine.Table(&Users{}).Cols("created_at").Find(&users)
	assert.NoError(t, err)

	for _, user := range users {
		layout := "2006-01-02 15:04:05"
		assert.EqualValues(t, curTime.Format(layout), user.Created.Format(layout))
	}
}
