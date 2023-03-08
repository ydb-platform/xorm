package ydb

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"xorm.io/builder"
	"xorm.io/xorm/schemas"
)

type UpsertUserA struct {
	Uuid int64 `xorm:"pk"`
	Msg  string
	Age  uint32
}

func (*UpsertUserA) TableName() string {
	return "upsert_user_a"
}

type UpsertUserB struct {
	UpsertUserA `xorm:"extends"`
}

func (*UpsertUserB) TableName() string {
	return "test/upsert_user_b"
}

func TestUpsertSinglePK(t *testing.T) {
	assert.NoError(t, PrepareScheme(&UpsertUserA{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()
	assert.NoError(t, session.Begin())
	defer session.Rollback()

	var insertData []*UpsertUserA
	for i := 0; i < 5; i++ {
		insertData = append(insertData, &UpsertUserA{
			Uuid: int64(i),
			Msg:  fmt.Sprintf("msg_%d", i),
			Age:  uint32(22),
		})
	}
	_, err = session.Insert(insertData)
	assert.NoError(t, err)

	assert.NoError(t, session.Commit())

	assert.NoError(t, session.Begin())

	var data []map[string]interface{}
	for i := 5; i < 10; i++ {
		data = append(data, map[string]interface{}{
			"uuid": int64(i),
			"msg":  fmt.Sprintf("msg_%d", i),
			"age":  uint32(22),
		})
	}
	_, err = session.Table((&UpsertUserA{}).TableName()).Upsert(data)
	assert.NoError(t, err)

	assert.NoError(t, session.Commit())

	cnt, err := session.Table((&UpsertUserA{}).TableName()).Count()
	assert.NoError(t, err)
	assert.EqualValues(t, 10, cnt)

	_, err = session.
		Table((&UpsertUserA{}).TableName()).
		Upsert(map[string]interface{}{
			"uuid": int64(5),
			"msg":  "upsert_msg",
		})
	assert.NoError(t, err)

	var ret UpsertUserA
	has, err := session.ID(int64(5)).Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.EqualValues(t, 5, ret.Uuid)
	assert.EqualValues(t, "upsert_msg", ret.Msg)
	assert.EqualValues(t, 22, ret.Age)
}

func TestUpsertSinglePKByFetch(t *testing.T) {
	t.Skip("FIXME")
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	assert.NoError(t, engine.Sync(&UpsertUserA{}, &UpsertUserB{}))

	session := engine.NewSession()
	defer session.Close()

	_, err = session.
		Table((&UpsertUserB{})).
		Upsert(builder.
			Select("uuid", "msg").
			From((&UpsertUserA{}).TableName()).
			Where(builder.Neq{"msg": "upsert_msg"}))
	assert.NoError(t, err)

	var ret UpsertUserB
	has, err := session.Where("msg = ?", "upsert_msg").Get(&ret)
	assert.NoError(t, err)
	assert.False(t, has)

	for i := 0; i < 10; i++ {
		ret = UpsertUserB{}
		has, err = session.ID(int64(i)).Get(&ret)
		assert.NoError(t, err)
		if i == 5 {
			assert.False(t, has)
		} else {
			assert.True(t, has)
			assert.EqualValues(t, i, ret.Uuid)
			assert.EqualValues(t, fmt.Sprintf("msg_%d", i), ret.Msg)
			assert.EqualValues(t, 0, ret.Age)
		}
	}
}

type UpsertUsers struct {
	Users `xorm:"extends"`
}

func (*UpsertUsers) TableName() string {
	return "upsert_users"
}

func TestUpsertCompositePK(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	assert.NoError(t, PrepareScheme(&UpsertUsers{}))
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	var dataUsers []map[string]interface{}
	var dataUpsertUsers []map[string]interface{}
	for i := 0; i < 20; i++ {
		dataUsers = append(dataUsers, map[string]interface{}{
			"name":       "datbeohbbh",
			"age":        uint32(22),
			"user_id":    sql.NullInt64{Int64: int64(i), Valid: true},
			"number":     uuid.NewString(),
			"created_at": time.Now(),
			"updated_at": time.Now(),
		})

		dataUpsertUsers = append(dataUpsertUsers, map[string]interface{}{
			"user_id": sql.NullInt64{Int64: int64(i), Valid: true},
			"number":  uuid.NewString(),
		})
	}

	_, err = session.
		Table((&Users{}).TableName()).
		Upsert(dataUsers)
	assert.NoError(t, err)

	_, err = session.
		Table((&UpsertUsers{}).TableName()).
		Upsert(dataUpsertUsers)
	assert.NoError(t, err)

	_, err = session.
		Table((&UpsertUsers{}).TableName()).
		Upsert(builder.Select("*").From((&Users{}).TableName()))
	assert.NoError(t, err)

	cnt, err := session.Table((&UpsertUsers{}).TableName()).Count()
	assert.NoError(t, err)
	assert.EqualValues(t, 40, cnt)

	assert.NoError(t, engine.NewSession().DropTable(&UpsertUsers{}))
	assert.NoError(t, engine.Sync(&UpsertUsers{}))

	for i := 0; i < 20; i++ {
		dataUpsertUsers = append(dataUpsertUsers, map[string]interface{}{
			"user_id": sql.NullInt64{Int64: int64(i), Valid: true},
			"number":  dataUsers[i]["number"],
		})
	}

	// after below upsert, table "upsert_users" is same as "users"
	_, err = session.
		Table((&UpsertUsers{}).TableName()).
		Upsert(builder.Select("*").From((&Users{}).TableName()))
	assert.NoError(t, err)

	cnt, err = session.Table((&UpsertUsers{}).TableName()).Count()
	assert.NoError(t, err)
	assert.EqualValues(t, 20, cnt)

	loc := engine.GetTZLocation()

	for i := 0; i < 20; i++ {
		var ret UpsertUsers
		has, err := session.
			ID(schemas.PK{
				sql.NullInt64{Int64: int64(i), Valid: true},
				dataUsers[i]["number"],
			}).
			Get(&ret)
		assert.NoError(t, err)
		assert.True(t, has)
		assert.NotNil(t, ret)
		assert.EqualValues(t, dataUsers[i]["name"], ret.Name)
		assert.EqualValues(t, dataUsers[i]["age"], ret.Age)
		assert.EqualValues(t, dataUsers[i]["user_id"], ret.UserID)
		assert.EqualValues(t, dataUsers[i]["number"], ret.Number)
		assert.EqualValues(t, dataUsers[i]["created_at"].(time.Time).In(loc).Format(time.RFC3339), ret.Created.In(loc).Format(time.RFC3339))
		assert.EqualValues(t, dataUsers[i]["updated_at"].(time.Time).In(loc).Format(time.RFC3339), ret.Updated.In(loc).Format(time.RFC3339))
	}
}
