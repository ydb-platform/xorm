package ydb

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"xorm.io/xorm/schemas"
)

func TestSetExpr(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()

	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	_, err = engine.
		SetExpr("age", uint32(100)).
		ID(schemas.PK{
			sql.NullInt64{Int64: 0, Valid: true},
			usersData[0].Number,
		}).
		Update(&Users{})
	assert.NoError(t, err)
}

func TestCols(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	user := Users{
		Name: "datbeohbbh",
		Age:  uint32(22),
		Account: Account{
			UserID: sql.NullInt64{Int64: 1, Valid: true},
			Number: uuid.NewString(),
		},
	}

	_, err = engine.Insert(&user)
	assert.NoError(t, err)

	_, err = engine.
		Cols("name").
		Cols("age").
		ID(schemas.PK{
			user.UserID,
			user.Number,
		}).
		Update(&Users{
			Name: "",
			Age:  uint32(0),
		})
	assert.NoError(t, err)

	ret := Users{}
	_, err = engine.
		ID(schemas.PK{
			user.UserID,
			user.Number,
		}).Get(&ret)
	assert.NoError(t, err)
	assert.EqualValues(t, "", ret.Name)
	assert.EqualValues(t, 0, ret.Age)
}

func TestMustCols(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	user := Users{
		Account: Account{
			UserID: sql.NullInt64{Int64: 1, Valid: true},
			Number: uuid.NewString(),
		},
	}

	_, err = engine.Insert(&user)
	assert.NoError(t, err)

	type OnlyUuid struct {
		UserId sql.NullInt64
	}

	updData := Users{
		Name: "datbeohbbh",
		Age:  uint32(22),
	}
	_, err = engine.
		MustCols("age").
		Update(&updData, &OnlyUuid{UserId: sql.NullInt64{Int64: 1, Valid: true}})
	assert.NoError(t, err)
}
