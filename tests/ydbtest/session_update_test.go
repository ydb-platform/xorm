package ydb

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"xorm.io/builder"
	"xorm.io/xorm/internal/statements"
	"xorm.io/xorm/schemas"
)

func TestUpdateMap(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	loc := engine.GetTZLocation()

	users := []map[string]interface{}{}

	for i := 0; i < 20; i++ {
		users = append(users, map[string]interface{}{
			"name":    fmt.Sprintf("Dat - %d", i),
			"age":     uint32(22 + i),
			"user_id": sql.NullInt64{Int64: int64(i + 1), Valid: true},
			"number":  uuid.NewString(),
		})
	}
	_, err = engine.Table((&Users{}).TableName()).Insert(users)
	assert.NoError(t, err)

	_, err = engine.
		Table((&Users{}).TableName()).
		Where(builder.Between{
			Col:     "user_id",
			LessVal: sql.NullInt64{Int64: 1, Valid: true},
			MoreVal: sql.NullInt64{Int64: 5, Valid: true},
		}).
		Update(map[string]interface{}{
			"name":       "datbeohbbh",
			"age":        uint32(22),
			"updated_at": time.Now().In(loc),
		})
	assert.NoError(t, err)

	_, err = engine.
		Table((&Users{}).TableName()).
		Update(map[string]interface{}{
			"name":       "datbeohbbh - test",
			"updated_at": time.Now().In(loc),
		}, &Users{
			Account: Account{UserID: sql.NullInt64{Int64: 6, Valid: true}},
		})
	assert.NoError(t, err)

	_, err = engine.
		Table((&Users{}).TableName()).
		ID(schemas.PK{
			sql.NullInt64{Int64: 7, Valid: true},
			users[6]["number"].(string),
		}).
		Update(map[string]interface{}{
			"name":       "datbeohbbh - test - 2",
			"updated_at": time.Now().In(loc),
		})
	assert.Error(t, err)
	assert.True(t, statements.IsIDConditionWithNoTableErr(err))

	_, err = engine.
		Table((&Users{}).TableName()).
		Where("user_id = ? AND number = ?",
			sql.NullInt64{Int64: 7, Valid: true},
			users[6]["number"].(string)).
		Update(map[string]interface{}{
			"name":       "datbeohbbh - test - 2",
			"updated_at": time.Now().In(loc),
		})
	assert.NoError(t, err)
}

func TestUpdateIn(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetDefaultEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	users := getUsersData()
	_, err = engine.Insert(&users)
	assert.NoError(t, err)

	userIds := []sql.NullInt64{}
	for i := 0; i < 10; i++ {
		userIds = append(userIds, sql.NullInt64{
			Int64: int64(i),
			Valid: true,
		})
	}
	_, err = engine.In("user_id", userIds).Update(&Users{
		Name: "datbeohbbh",
		Age:  uint32(22),
	})
	assert.NoError(t, err)
}

func TestUpdateStruct(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetDefaultEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	users := getUsersData()
	_, err = engine.Insert(&users)
	assert.NoError(t, err)

	user := Users{
		Name: "datbeohbbh",
		Age:  uint32(22),
	}
	_, err = engine.
		ID(schemas.PK{
			sql.NullInt64{Int64: 0, Valid: true},
			users[0].Number,
		}).
		Update(&user)
	assert.NoError(t, err)

	_, err = engine.Update(&user, &Users{
		Account: Account{
			Number: users[0].Number,
		},
	})
	assert.NoError(t, err)
}

func TestUpdateIncrDecr(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetDefaultEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	users := getUsersData()
	_, err = engine.Insert(&users)
	assert.NoError(t, err)

	userIncr := Users{
		Name: "datbeohbbh - incr",
	}

	_, err = engine.
		ID(schemas.PK{
			sql.NullInt64{Int64: 0, Valid: true},
			users[0].Number,
		}).
		Incr("age", uint32(10)).
		Update(&userIncr)
	assert.NoError(t, err)

	userDecr := Users{
		Name: "datbeohbbh - decr",
	}
	_, err = engine.
		ID(schemas.PK{
			sql.NullInt64{Int64: 1, Valid: true},
			users[1].Number,
		}).
		Decr("age", uint32(10)).
		Update(&userDecr)
	assert.NoError(t, err)
}

func TestUpdateMapCondition(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetDefaultEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	users := getUsersData()
	_, err = engine.Insert(&users)
	assert.NoError(t, err)

	user := Users{
		Name: "datbeohbbh",
	}

	_, err = engine.Update(&user, map[string]interface{}{
		"user_id": sql.NullInt64{Int64: 0, Valid: true},
		"number":  users[0].Number,
	})
	assert.NoError(t, err)

	ret := Users{}
	has, err := engine.ID(schemas.PK{
		sql.NullInt64{Int64: 0, Valid: true},
		users[0].Number,
	}).Get(&ret)

	assert.NoError(t, err)
	assert.True(t, has)
	assert.Equal(t, user.Name, ret.Name)
}

func TestUpdateExprs(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetDefaultEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	users := getUsersData()
	_, err = engine.Insert(&users)
	assert.NoError(t, err)

	user := Users{
		Name: "datbeohbbh",
	}

	_, err = engine.
		SetExpr("age", uint32(0)).
		// AllCols().
		Update(&user)
	assert.NoError(t, err)
}

func TestUpdateExprs2(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetDefaultEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	users := getUsersData()
	_, err = engine.Insert(&users)
	assert.NoError(t, err)

	_, err = engine.
		SetExpr("age", uint32(20)).
		SetExpr("name", "\"test\"").
		Where(builder.Gte{
			"user_id": sql.NullInt64{Int64: 5, Valid: true},
		}).
		Where(builder.Lt{
			"user_id": sql.NullInt64{Int64: 10, Valid: true},
		}).
		And("age > ?", uint32(22)).
		Update(new(Users))
	assert.NoError(t, err)
}
