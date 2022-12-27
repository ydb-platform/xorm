package ydb

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"xorm.io/xorm/schemas"
)

func TestExistStruct(t *testing.T) {
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	user := Users{
		Name: "datbeohbbh",
		Age:  uint32(22),
		Account: Account{
			UserID: sql.NullInt64{Int64: 22, Valid: true},
			Number: uuid.NewString(),
		},
	}

	session := engine.NewSession()
	defer session.Close()

	err = session.DropTable(new(Users))
	assert.NoError(t, err)

	has, err := session.Exist(new(Users))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Cannot find table")
	assert.False(t, has)

	err = session.CreateTable(&Users{})
	assert.NoError(t, err)

	_, err = session.Insert(&user)
	assert.NoError(t, err)

	has, err = session.Exist(new(Users))
	assert.NoError(t, err)
	assert.True(t, has)

	has, err = session.Exist(&Users{
		Name: "datbeohbbh",
		Age:  uint32(22),
	})
	assert.NoError(t, err)
	assert.True(t, has)

	has, err = session.Exist(&Users{
		Name: "datbeohbbh-non-exist",
		Age:  uint32(22),
	})
	assert.NoError(t, err)
	assert.False(t, has)

	has, err = session.Where("name = ?", "datbeohbbh").Exist(&Users{})
	assert.NoError(t, err)
	assert.True(t, has)

	has, err = session.Where("name = ?", "datbeohbbh-test").Exist(&Users{})
	assert.NoError(t, err)
	assert.False(t, has)

	has, err = session.
		SQL("SELECT * FROM users WHERE name = ? AND age = ?", "datbeohbbh", uint32(22)).
		Exist()
	assert.NoError(t, err)
	assert.True(t, has)

	has, err = session.
		SQL("SELECT * FROM users WHERE name = ? AND age = ?", "datbeohbbh-test", uint32(22)).
		Exist()
	assert.NoError(t, err)
	assert.False(t, has)

	has, err = session.Table("users").Exist()
	assert.NoError(t, err)
	assert.True(t, has)

	has, err = session.Table("users").Where("name = ?", "datbeohbbh").Exist()
	assert.NoError(t, err)
	assert.True(t, has)

	has, err = session.Table("users").Where("name = ?", "datbeohbbh-test").Exist()
	assert.NoError(t, err)
	assert.False(t, has)

	has, err = session.Table(new(Users)).
		ID(
			schemas.PK{
				sql.NullInt64{Int64: 22, Valid: true},
				user.Number,
			},
		).
		Cols("number").
		Exist(&Users{})
	assert.NoError(t, err)
	assert.True(t, has)
}

func TestExistStructForJoin(t *testing.T) {
	type Number struct {
		Uuid []byte `xorm:"pk"`
		Lid  []byte
	}

	type OrderList struct {
		Uuid []byte `xorm:"pk"`
		Eid  []byte
	}

	type Player struct {
		Uuid []byte `xorm:"pk"`
		Name string
	}

	assert.NoError(t, PrepareScheme(&Number{}))
	assert.NoError(t, PrepareScheme(&OrderList{}))
	assert.NoError(t, PrepareScheme(&Player{}))

	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	defer func() { // clean up
		session := engine.NewSession()
		assert.NoError(t, session.DropTable(&Number{}))
		assert.NoError(t, session.DropTable(&OrderList{}))
		assert.NoError(t, session.DropTable(&Player{}))
		session.Close()
	}()

	ply := Player{
		Uuid: []byte(uuid.NewString()),
		Name: "datbeohbbh",
	}
	_, err = engine.Insert(&ply)
	assert.NoError(t, err)

	var orderlist = OrderList{
		Uuid: []byte(uuid.NewString()),
		Eid:  ply.Uuid,
	}
	_, err = engine.Insert(&orderlist)
	assert.NoError(t, err)

	var um = Number{
		Uuid: []byte(uuid.NewString()),
		Lid:  orderlist.Uuid,
	}
	_, err = engine.Insert(&um)
	assert.NoError(t, err)

	session := engine.NewSession()
	defer session.Close()

	session.Table("number").
		Join("INNER", "order_list", "`order_list`.`uuid` = `number`.`lid`").
		Join("LEFT", "player", "`player`.`uuid` = `order_list`.`eid`").
		Where("`number`.`lid` = ?", um.Lid)
	has, err := session.Exist()
	assert.NoError(t, err)
	assert.True(t, has)

	session.Table("number").
		Join("INNER", "order_list", "`order_list`.`uuid` = `number`.`lid`").
		Join("LEFT", "player", "`player`.`uuid` = `order_list`.`eid`").
		Where("`number`.`lid` = ?", []byte("fake data"))
	has, err = session.Exist()
	assert.NoError(t, err)
	assert.False(t, has)

	session.Table("number").
		Select("`order_list`.`uuid`").
		Join("INNER", "order_list", "`order_list`.`uuid` = `number`.`lid`").
		Join("LEFT", "player", "`player`.`uuid` = `order_list`.`eid`").
		Where("`order_list`.`uuid` = ?", orderlist.Uuid)
	has, err = session.Exist()
	assert.NoError(t, err)
	assert.True(t, has)

	session.Table("number").
		Select("player.uuid").
		Join("INNER", "order_list", "`order_list`.`uuid` = `number`.`lid`").
		Join("LEFT", "player", "`player`.`uuid` = `order_list`.`eid`").
		Where("`player`.`uuid` = ?", []byte("fake data"))
	has, err = session.Exist()
	assert.NoError(t, err)
	assert.False(t, has)

	session.Table("number").
		Select("player.uuid").
		Join("INNER", "order_list", "`order_list`.`uuid` = `number`.`lid`").
		Join("LEFT", "player", "`player`.`uuid` = `order_list`.`eid`")
	has, err = session.Exist()
	assert.NoError(t, err)
	assert.True(t, has)

	err = session.DropTable("order_list")
	assert.NoError(t, err)

	exist, err := session.IsTableExist("order_list")
	assert.NoError(t, err)
	assert.False(t, exist)

	session.Table("number").
		Select("player.uuid").
		Join("INNER", "order_list", "`order_list`.`uuid` = `number`.`lid`").
		Join("LEFT", "player", "`player`.`uuid` = `order_list`.`eid`")
	has, err = session.Exist()
	assert.Error(t, err)
	assert.False(t, has)

	session.Table("number").
		Select("player.uuid").
		Join("LEFT", "player", "`player`.`uuid` = `number`.`lid`")
	has, err = session.Exist()
	assert.NoError(t, err)
	assert.True(t, has)
}

func TestExistContext(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	user := Users{
		Name: "datbeohbbh",
		Age:  uint32(22),
		Account: Account{
			UserID: sql.NullInt64{Int64: 22, Valid: true},
			Number: uuid.NewString(),
		},
	}

	_, err = engine.Insert(&user)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()

	time.Sleep(time.Nanosecond)

	has, err := engine.Context(ctx).Exist(&user)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")
	assert.False(t, has)
}
