package ydb

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"xorm.io/builder"
	"xorm.io/xorm"
)

func TestDelete(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	users := getUsersData()

	_, err = engine.Transaction(func(session *xorm.Session) (interface{}, error) {
		_, err = session.Insert(users)
		return nil, err
	})
	assert.NoError(t, err)

	_, err = engine.Transaction(func(session *xorm.Session) (interface{}, error) {
		_, err = session.Delete(&Users{
			Account: Account{
				UserID: sql.NullInt64{Int64: 1, Valid: true},
			},
		})
		return nil, err
	})
	assert.NoError(t, err)

	user := Users{}
	has, err := engine.Where("user_id = ?", sql.NullInt64{Int64: 1, Valid: true}).Get(&user)
	assert.NoError(t, err)
	assert.False(t, has)

	_, err = engine.Transaction(func(session *xorm.Session) (interface{}, error) {
		_, err = session.Table((&Users{}).TableName()).Where(builder.Between{
			Col:     "user_id",
			LessVal: sql.NullInt64{Int64: 10, Valid: true},
			MoreVal: sql.NullInt64{Int64: 15, Valid: true},
		}).Delete()
		return nil, err
	})
	assert.NoError(t, err)

	cnt, err := engine.Table(&Users{}).Where(builder.Between{
		Col:     "user_id",
		LessVal: sql.NullInt64{Int64: 10, Valid: true},
		MoreVal: sql.NullInt64{Int64: 15, Valid: true},
	}).Count()
	assert.NoError(t, err)
	assert.EqualValues(t, 0, cnt)

	has, err = engine.Get(&Users{
		Account: Account{
			UserID: sql.NullInt64{Int64: 0, Valid: true},
		},
	})
	assert.NoError(t, err)
	assert.True(t, has)
}

// FIXME: failed on get `deleted` field, unknown problem that return argument
// of time.Time type as string type.
/* type UsersDeleted struct {
	Name    string    `xorm:"'name'"`
	Age     uint32    `xorm:"'age'"`
	Deleted time.Time `xorm:"deleted"`
	Account `xorm:"extends"`
}

func (*UsersDeleted) TableName() string {
	return "users"
}

// !datbeohbbh! not supported
func TestDeleted(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)

	assert.NoError(t, engine.Sync(&UsersDeleted{}))

	user := UsersDeleted{
		Name: "datbeohbbh",
		Age:  uint32(22),
		Account: Account{
			UserID: sql.NullInt64{Int64: 1, Valid: true},
			Number: uuid.NewString(),
		},
	}

	session := engine.NewSession()
	defer session.Close()

	_, err = session.Insert(&user)
	assert.NoError(t, err)

	ret := UsersDeleted{}
	has, err := session.Get(&UsersDeleted{
		Account: Account{
			UserID: sql.NullInt64{Int64: 1, Valid: true},
		},
	})
	assert.NoError(t, err)
	assert.True(t, has)

	_, err = session.Delete(&UsersDeleted{
		Account: Account{
			UserID: sql.NullInt64{Int64: 1, Valid: true},
		},
	})
	assert.NoError(t, err)

	ret = UsersDeleted{}
	has, err = session.Get(&UsersDeleted{
		Account: Account{
			UserID: sql.NullInt64{Int64: 1, Valid: true},
		},
	})
	assert.NoError(t, err)
	assert.False(t, has)

	_, err = session.Delete(&UsersDeleted{
		Account: Account{
			UserID: sql.NullInt64{Int64: 1, Valid: true},
		},
	})
	assert.NoError(t, err)

	ret = UsersDeleted{}
	has, err = session.
		Unscoped().
		Where("user_id = ?", sql.NullInt64{Int64: 1, Valid: true}).
		Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)

	_, err = session.
		Table("users").
		Unscoped().
		Where("user_id = ?", sql.NullInt64{Int64: 1, Valid: true}).
		Delete()
	assert.NoError(t, err)

	ret = UsersDeleted{}
	has, err = session.
		Unscoped().
		Where("user_id = ?", sql.NullInt64{Int64: 1, Valid: true}).
		Get(&ret)
	assert.NoError(t, err)
	assert.False(t, has)
}
*/
