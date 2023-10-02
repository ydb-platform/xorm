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

func TestYQLUpsertSinglePK(t *testing.T) {
	assert.NoError(t, PrepareScheme(&UpsertUserA{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	_, err = session.Insert([]*UpsertUserA{
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
			"UPSERT INTO `upsert_user_a` (`uuid`, `msg`, `age`) VALUES "+
				"($1, $2, $3), ($4, $5, $6);",
			int64(3), "msg_3", uint32(22),
			int64(4), "msg_4", uint32(22),
		)

	assert.NoError(t, err)

	cnt, err := session.Table((&UpsertUserA{}).TableName()).Count()
	assert.NoError(t, err)
	assert.EqualValues(t, 4, cnt)

	_, err = session.
		Exec(
			"UPSERT INTO `upsert_user_a` (`uuid`, `msg`) VALUES "+
				"($1, $2);",
			int64(1), "upsert_msg",
		)
	assert.NoError(t, err)

	var ret UpsertUserA
	has, err := session.ID(int64(1)).Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.EqualValues(t, 1, ret.Uuid)
	assert.EqualValues(t, "upsert_msg", ret.Msg)
	assert.EqualValues(t, uint32(22), ret.Age) // value are preserved
}

func TestYQLUpsertSinglePKByFetch(t *testing.T) {
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	assert.NoError(t, engine.Sync(&UpsertUserA{}, &UpsertUserB{}))

	session := engine.NewSession()
	defer session.Close()

	_, err = session.
		Exec("UPSERT INTO `test/upsert_user_b` (`uuid`, `msg`, `age`) "+
			"SELECT `uuid`, `msg`, `age` FROM `upsert_user_a` WHERE `msg` = $1;", "upsert_msg")
	assert.NoError(t, err)

	var ret UpsertUserB
	has, err := session.Table(&UpsertUserB{}).Where("msg = ?", "upsert_msg").Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)
}

type UpsertUsers struct {
	Users `xorm:"extends"`
}

func (*UpsertUsers) TableName() string {
	return "upsert_users"
}

func TestYQLUpsertCompositePK(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}, &UpsertUsers{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	loc := engine.GetTZLocation()

	session := engine.NewSession()
	defer session.Close()

	uuidArg := uuid.NewString()
	now := sql.NullTime{Time: time.Now().In(loc), Valid: true}

	_, err = session.
		Exec("UPSERT INTO `users` (`name`, `age`, `user_id`, `number`, `created_at`, `updated_at`) "+
			"VALUES ($1, $2, $3, $4, $5, $6);", "datbeohbbh", uint32(22), sql.NullInt64{Int64: int64(1), Valid: true}, uuidArg, now, now)
	assert.NoError(t, err)

	_, err = session.
		Exec("UPSERT INTO `upsert_users` (`user_id`, `number`,`name`) "+
			"VALUES ($1, $2, $3);", sql.NullInt64{Int64: int64(1), Valid: true}, uuidArg, "test")
	assert.NoError(t, err)

	_, err = session.Exec("UPSERT INTO `upsert_users` SELECT `user_id`, `number`, `age`, `created_at`, `updated_at` FROM `users`;")
	assert.NoError(t, err)

	cnt, err := session.Table((&UpsertUsers{}).TableName()).Count()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, cnt)

	var ret UpsertUsers
	has, err := session.
		Table(&UpsertUsers{}).
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
	assert.EqualValues(t, "test", ret.Name) // value of column `name` is preserved

	// values are updated after fetched
	assert.EqualValues(t, 22, ret.Age)
	assert.EqualValues(t, now.Time.In(loc).Format(time.RFC3339), ret.Created.Time.Format(time.RFC3339))
	assert.EqualValues(t, now.Time.In(loc).Format(time.RFC3339), ret.Updated.Time.Format(time.RFC3339))
}
