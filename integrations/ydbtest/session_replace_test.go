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

type ReplaceUserA struct {
	Uuid int64 `xorm:"pk"`
	Msg  string
	Age  uint32
}

func (*ReplaceUserA) TableName() string {
	return "replace_user_a"
}

type ReplaceUserB struct {
	ReplaceUserA `xorm:"extends"`
}

func (*ReplaceUserB) TableName() string {
	return "test/replace_user_b"
}

func TestReplaceSinglePK(t *testing.T) {
	assert.NoError(t, PrepareScheme(&ReplaceUserA{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()
	assert.NoError(t, session.Begin())
	defer session.Rollback()

	var insertData []*ReplaceUserA
	for i := 0; i < 5; i++ {
		insertData = append(insertData, &ReplaceUserA{
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
	_, err = session.Table((&ReplaceUserA{}).TableName()).Replace(data)
	assert.NoError(t, err)

	assert.NoError(t, session.Commit())

	cnt, err := session.Table((&ReplaceUserA{}).TableName()).Count()
	assert.NoError(t, err)
	assert.EqualValues(t, 10, cnt)

	_, err = session.
		Table((&ReplaceUserA{}).TableName()).
		Replace(map[string]interface{}{
			"uuid": int64(5),
			"msg":  "replace_msg",
		})
	assert.NoError(t, err)

	var ret ReplaceUserA
	has, err := session.ID(int64(5)).Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.EqualValues(t, 5, ret.Uuid)
	assert.EqualValues(t, "replace_msg", ret.Msg)
	assert.EqualValues(t, 0, ret.Age) // replace with default value
}

func TestReplaceSinglePKByFetch(t *testing.T) {
	t.Skip("FIXME")
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	assert.NoError(t, engine.Sync(&ReplaceUserA{}, &ReplaceUserB{}))

	session := engine.NewSession()
	defer session.Close()

	_, err = session.
		Table((&ReplaceUserB{})).
		Replace(builder.
			Select("uuid", "msg", "age").
			From((&ReplaceUserA{}).TableName()).
			Where(builder.Neq{"msg": "replace_msg"}))
	assert.NoError(t, err)

	var ret ReplaceUserB
	has, err := session.Where("msg = ?", "replace_msg").Get(&ret)
	assert.NoError(t, err)
	assert.False(t, has)

	for i := 0; i < 10; i++ {
		ret = ReplaceUserB{}
		has, err = session.ID(int64(i)).Get(&ret)
		assert.NoError(t, err)
		if i == 5 {
			assert.False(t, has)
		} else {
			assert.True(t, has)
			assert.EqualValues(t, i, ret.Uuid)
			assert.EqualValues(t, fmt.Sprintf("msg_%d", i), ret.Msg)
			assert.EqualValues(t, 22, ret.Age)
		}
	}
}

type ReplaceUsers struct {
	Users `xorm:"extends"`
}

func (*ReplaceUsers) TableName() string {
	return "replace_users"
}

func TestReplaceCompositePK(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	assert.NoError(t, PrepareScheme(&ReplaceUsers{}))
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	var dataUsers []map[string]interface{}
	var dataReplaceUsers []map[string]interface{}
	for i := 0; i < 20; i++ {
		dataUsers = append(dataUsers, map[string]interface{}{
			"name":       "datbeohbbh",
			"age":        uint32(22),
			"user_id":    sql.NullInt64{Int64: int64(i), Valid: true},
			"number":     uuid.NewString(),
			"created_at": time.Now(),
			"updated_at": time.Now(),
		})

		dataReplaceUsers = append(dataReplaceUsers, map[string]interface{}{
			"user_id": sql.NullInt64{Int64: int64(i), Valid: true},
			"number":  uuid.NewString(),
		})
	}

	_, err = session.
		Table((&Users{}).TableName()).
		Replace(dataUsers)
	assert.NoError(t, err)

	_, err = session.
		Table((&ReplaceUsers{}).TableName()).
		Replace(dataReplaceUsers)
	assert.NoError(t, err)

	_, err = session.
		Table((&ReplaceUsers{}).TableName()).
		Replace(builder.Select("*").From((&Users{}).TableName()))
	assert.NoError(t, err)

	cnt, err := session.Table((&ReplaceUsers{}).TableName()).Count()
	assert.NoError(t, err)
	assert.EqualValues(t, 40, cnt)

	assert.NoError(t, engine.NewSession().DropTable(&ReplaceUsers{}))
	assert.NoError(t, engine.Sync(&ReplaceUsers{}))

	for i := 0; i < 20; i++ {
		dataReplaceUsers = append(dataReplaceUsers, map[string]interface{}{
			"user_id": sql.NullInt64{Int64: int64(i), Valid: true},
			"number":  dataUsers[i]["number"],
		})
	}

	_, err = session.
		Table((&ReplaceUsers{}).TableName()).
		Replace(builder.Select("user_id", "number", "name").From((&Users{}).TableName()))
	assert.NoError(t, err)

	cnt, err = session.Table((&ReplaceUsers{}).TableName()).Count()
	assert.NoError(t, err)
	assert.EqualValues(t, 20, cnt)

	for i := 0; i < 20; i++ {
		var ret ReplaceUsers
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
		assert.EqualValues(t, dataUsers[i]["user_id"], ret.UserID)
		assert.EqualValues(t, dataUsers[i]["number"], ret.Number)
		assert.EqualValues(t, 0, ret.Age)
		assert.EqualValues(t, time.Time{}.Format(time.RFC3339), ret.Created.Format(time.RFC3339))
		assert.EqualValues(t, time.Time{}.Format(time.RFC3339), ret.Updated.Format(time.RFC3339))
	}
}

func TestReplaceCompositePKDefaultValues(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	assert.NoError(t, PrepareScheme(&ReplaceUsers{}))
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	var dataUsers []map[string]interface{}
	var dataReplaceUsers []map[string]interface{}
	for i := 0; i < 20; i++ {
		uuidStr := uuid.NewString()
		dataUsers = append(dataUsers, map[string]interface{}{
			"name":       "datbeohbbh_users",
			"age":        uint32(22),
			"user_id":    sql.NullInt64{Int64: int64(i), Valid: true},
			"number":     uuidStr,
			"created_at": time.Now(),
			"updated_at": time.Now(),
		})

		dataReplaceUsers = append(dataReplaceUsers, map[string]interface{}{
			"name":       "datbeohbbh_replace_users",
			"age":        uint32(22),
			"user_id":    sql.NullInt64{Int64: int64(i), Valid: true},
			"number":     uuidStr,
			"created_at": time.Now(),
			"updated_at": time.Now(),
		})
	}

	_, err = session.
		Table((&Users{}).TableName()).
		Replace(dataUsers)
	assert.NoError(t, err)

	_, err = session.
		Table((&ReplaceUsers{}).TableName()).
		Replace(dataReplaceUsers)
	assert.NoError(t, err)

	_, err = session.
		Table((&ReplaceUsers{}).TableName()).
		Replace(builder.Select("user_id", "number").From((&Users{}).TableName()))
	assert.NoError(t, err)

	for i := 0; i < 20; i++ {
		var ret ReplaceUsers
		has, err := session.
			ID(schemas.PK{
				sql.NullInt64{Int64: int64(i), Valid: true},
				dataUsers[i]["number"],
			}).
			Get(&ret)
		assert.NoError(t, err)
		assert.True(t, has)
		assert.NotNil(t, ret)
		assert.EqualValues(t, dataUsers[i]["user_id"], ret.UserID)
		assert.EqualValues(t, dataUsers[i]["number"], ret.Number)
		// overwritten with default value
		assert.EqualValues(t, "", ret.Name)
		assert.EqualValues(t, 0, ret.Age)
		assert.EqualValues(t, time.Time{}.Format(time.RFC3339), ret.Created.Format(time.RFC3339))
		assert.EqualValues(t, time.Time{}.Format(time.RFC3339), ret.Updated.Format(time.RFC3339))
	}
}
