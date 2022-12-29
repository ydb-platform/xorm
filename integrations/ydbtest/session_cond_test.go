package ydb

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"xorm.io/builder"
)

func TestBuilder(t *testing.T) {
	const (
		OpEqual int32 = iota
		OpGreatThan
		OpLessThan
	)

	type Condition struct {
		CondId    int64 `xorm:"pk"`
		TableName string
		ColName   string
		Op        int32
		Value     string
	}

	assert.NoError(t, PrepareScheme(&Condition{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	_, err = engine.Insert(&Condition{CondId: int64(1), TableName: "table1", ColName: "col1", Op: OpEqual, Value: "1"})
	assert.NoError(t, err)

	var cond Condition
	var q = engine.Quote
	has, err := engine.Where(builder.Eq{q("col_name"): "col1"}).Get(&cond)
	assert.NoError(t, err)
	assert.Equal(t, true, has, "records should exist")

	has, err = engine.Where(builder.Eq{q("col_name"): "col1"}.
		And(builder.Eq{q("op"): OpEqual})).
		NoAutoCondition().
		Get(&cond)
	assert.NoError(t, err)
	assert.Equal(t, true, has, "records should exist")

	has, err = engine.Where(builder.Eq{q("col_name"): "col1", q("op"): OpEqual, q("value"): "1"}).
		NoAutoCondition().
		Get(&cond)
	assert.NoError(t, err)
	assert.Equal(t, true, has, "records should exist")

	has, err = engine.Where(builder.Eq{q("col_name"): "col1"}.
		And(builder.Neq{q("op"): OpEqual})).
		NoAutoCondition().
		Get(&cond)
	assert.NoError(t, err)
	assert.Equal(t, false, has, "records should not exist")

	var conds []Condition
	err = engine.Where(builder.Eq{q("col_name"): "col1"}.
		And(builder.Eq{q("op"): OpEqual})).
		Find(&conds)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(conds), "records should exist")

	conds = make([]Condition, 0)
	err = engine.Where(builder.Like{q("col_name"), "col"}).Find(&conds)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(conds), "records should exist")

	conds = make([]Condition, 0)
	err = engine.Where(builder.Expr(q("col_name")+" = ?", "col1")).Find(&conds)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(conds), "records should exist")

	conds = make([]Condition, 0)
	err = engine.Where(builder.In(q("col_name"), "col1", "col2")).Find(&conds)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(conds), "records should exist")

	conds = make([]Condition, 0)
	err = engine.NotIn("col_name", "col1", "col2").Find(&conds)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, len(conds), "records should not exist")

	// complex condtions
	var where = builder.NewCond()
	if true {
		where = where.And(builder.Eq{q("col_name"): "col1"})
		where = where.Or(builder.And(builder.In(q("col_name"), "col1", "col2"), builder.Expr(q("col_name")+" = ?", "col1")))
	}

	conds = make([]Condition, 0)
	err = engine.Where(where).Find(&conds)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(conds), "records should exist")
}

func TestIn(t *testing.T) {
	type Userinfo struct {
		Uid        int64 `xorm:"pk"`
		Username   string
		Departname string
	}

	assert.NoError(t, PrepareScheme(&Userinfo{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	_, err = engine.Insert([]Userinfo{
		{
			Uid:        int64(1),
			Username:   "user1",
			Departname: "dev",
		},
		{
			Uid:        int64(2),
			Username:   "user2",
			Departname: "dev",
		},
		{
			Uid:        int64(3),
			Username:   "user3",
			Departname: "dev",
		},
	})
	assert.NoError(t, err)

	department := "`" + engine.GetColumnMapper().Obj2Table("Departname") + "`"
	var usrs []Userinfo
	err = engine.Where(department+" = ?", "dev").Limit(3).Find(&usrs)
	assert.NoError(t, err)
	assert.EqualValues(t, 3, len(usrs))

	var ids []int64
	var idsStr string
	for _, u := range usrs {
		ids = append(ids, u.Uid)
		idsStr = fmt.Sprintf("%d,", u.Uid)
	}
	idsStr = idsStr[:len(idsStr)-1]

	users := make([]Userinfo, 0)
	err = engine.In("uid", ids[0], ids[1], ids[2]).Find(&users)
	assert.NoError(t, err)
	assert.EqualValues(t, 3, len(users))

	users = make([]Userinfo, 0)
	err = engine.In("uid", ids).Find(&users)
	assert.NoError(t, err)
	assert.EqualValues(t, 3, len(users))

	for _, user := range users {
		if user.Uid != ids[0] && user.Uid != ids[1] && user.Uid != ids[2] {
			err = errors.New("in uses should be " + idsStr + " total 3")
			assert.NoError(t, err)
		}
	}

	users = make([]Userinfo, 0)
	var idsInterface []interface{}
	for _, id := range ids {
		idsInterface = append(idsInterface, id)
	}

	err = engine.Where(department+" = ?", "dev").In("uid", idsInterface...).Find(&users)
	assert.NoError(t, err)
	assert.EqualValues(t, 3, len(users))

	for _, user := range users {
		if user.Uid != ids[0] && user.Uid != ids[1] && user.Uid != ids[2] {
			err = errors.New("in uses should be " + idsStr + " total 3")
			assert.NoError(t, err)
		}
	}

	dev := engine.GetColumnMapper().Obj2Table("Dev")

	err = engine.In("uid", int64(1)).In("uid", int64(2)).In(department, dev).Find(&users)
	assert.NoError(t, err)

	_, err = engine.In("uid", ids[0]).Update(&Userinfo{Departname: "dev-"})
	assert.NoError(t, err)

	user := new(Userinfo)
	has, err := engine.ID(ids[0]).Get(user)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.EqualValues(t, "dev-", user.Departname)

	_, err = engine.In("uid", ids[0]).Update(&Userinfo{Departname: "dev"})
	assert.NoError(t, err)

	_, err = engine.In("uid", ids[1]).Delete(&Userinfo{})
	assert.NoError(t, err)
}

func TestIn2(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()

	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	ids := []sql.NullInt64{}
	for i := 10; i < 20; i++ {
		ids = append(ids, sql.NullInt64{Int64: int64(i), Valid: true})
	}

	users := []Users{}
	cond := builder.In("user_id", ids)
	err = engine.Where(cond).Find(&users)
	assert.NoError(t, err)
	assert.EqualValues(t, len(ids), len(users))

	for i, user := range users {
		assert.Equal(t, sql.NullInt64{Int64: int64(i + 10), Valid: true}, user.UserID)
	}
}
