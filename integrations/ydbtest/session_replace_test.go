package ydb

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
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

func TestYQLReplaceSinglePK(t *testing.T) {
	assert.NoError(t, PrepareScheme(&ReplaceUserA{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	_, err = session.Insert([]*ReplaceUserA{
		{
			Uuid: int64(1),
			Msg:  fmt.Sprintf("msg_%d", 1),
			Age:  uint32(22),
		},
		{
			Uuid: int64(2),
			Msg:  fmt.Sprintf("msg_%d", 2),
			Age:  uint32(22),
		},
	})
	assert.NoError(t, err)

	_, err = session.
		Exec(
			"REPLACE INTO `replace_user_a` (`uuid`, `msg`, `age`) VALUES "+
				"($1, $2, $3), ($4, $5, $6);",
			int64(3), "msg_3", uint32(22), int64(4), "msg_4", uint32(22),
		)

	assert.NoError(t, err)

	cnt, err := session.Table((&ReplaceUserA{}).TableName()).Count()
	assert.NoError(t, err)
	assert.EqualValues(t, 4, cnt)

	_, err = session.
		Exec(
			"REPLACE INTO `replace_user_a` (`uuid`, `msg`) VALUES "+
				"($1, $2);",
			int64(1), "replace_msg",
		)
	assert.NoError(t, err)

	var ret ReplaceUserA
	has, err := session.ID(int64(1)).Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.EqualValues(t, 1, ret.Uuid)
	assert.EqualValues(t, "replace_msg", ret.Msg)
	assert.EqualValues(t, 0, ret.Age) // replace with default value
}

func TestYQLReplaceSinglePKByFetch(t *testing.T) {
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	assert.NoError(t, engine.Sync(&ReplaceUserA{}, &ReplaceUserB{}))

	session := engine.NewSession()
	defer session.Close()

	_, err = session.
		Exec("REPLACE INTO `test/replace_user_b` (`uuid`, `msg`, `age`) "+
			"SELECT `uuid`, `msg`, `age` FROM `replace_user_a` WHERE `msg` = $1;", "replace_msg")
	assert.NoError(t, err)

	var ret ReplaceUserB
	has, err := session.Table(&ReplaceUserB{}).Where("msg = ?", "replace_msg").Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)
}

type ReplaceUsers struct {
	Users `xorm:"extends"`
}

func (*ReplaceUsers) TableName() string {
	return "replace_users"
}

func TestYQLReplaceCompositePK(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	assert.NoError(t, PrepareScheme(&ReplaceUsers{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	uuidArg := uuid.NewString()
	now := time.Now()

	_, err = session.
		Exec("REPLACE INTO `users` (`name`, `age`, `user_id`, `number`, `created_at`, `updated_at`) "+
			"VALUES ($1, $2, $3, $4, $5, $6);", "datbeohbbh", uint32(22), sql.NullInt64{Int64: int64(1), Valid: true}, uuidArg, now, now)
	assert.NoError(t, err)

	_, err = session.
		Exec("REPLACE INTO `replace_users` (`user_id`, `number`) "+
			"VALUES ($1, $2);", sql.NullInt64{Int64: int64(1), Valid: true}, uuidArg)
	assert.NoError(t, err)

	_, err = session.Exec("REPLACE INTO `replace_users` SELECT `user_id`, `number`, `name` FROM `users`;")
	assert.NoError(t, err)

	cnt, err := session.Table((&ReplaceUsers{}).TableName()).Count()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, cnt)

	var ret ReplaceUsers
	has, err := session.
		Table(&ReplaceUsers{}).
		ID(schemas.PK{
			sql.NullInt64{Int64: int64(1), Valid: true},
			uuidArg,
		}).
		Get(&ret)

	assert.NoError(t, err)

	assert.True(t, has)
	assert.NotNil(t, ret)

	assert.EqualValues(t, int64(1), ret.UserID.Int64)
	assert.EqualValues(t, uuidArg, ret.Number)
	assert.EqualValues(t, "datbeohbbh", ret.Name)

	// overwritten with default values
	assert.EqualValues(t, 0, ret.Age)
	assert.EqualValues(t, time.Time{}.Format(time.RFC3339), ret.Created.Format(time.RFC3339))
	assert.EqualValues(t, time.Time{}.Format(time.RFC3339), ret.Updated.Format(time.RFC3339))
}
