package ydb

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"xorm.io/xorm"
)

func TestRowsStruct(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	users := getUsersData()

	session := engine.NewSession()
	defer session.Close()

	_, err = session.Insert(users)
	assert.NoError(t, err)

	rows, err := session.Asc("user_id").Rows(&Users{})
	assert.NoError(t, err)
	defer rows.Close()

	for i := 0; rows.Next(); i++ {
		var user Users
		assert.NoError(t, rows.Scan(&user))
		assert.Equal(t, users[i].UserID, user.UserID)
		assert.Equal(t, users[i].Number, user.Number)
	}
}

func TestRowsCond(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	users := getUsersData()

	session := engine.NewSession()
	defer session.Close()

	_, err = session.Insert(users)
	assert.NoError(t, err)

	timeoutCtx, cancelFunc := context.WithTimeout(enginePool.ctx, 10*time.Second)
	defer cancelFunc()

	result, err := engine.TransactionContext(timeoutCtx, func(ctx context.Context, session *xorm.Session) (interface{}, error) {
		rows, err := session.
			Cols("user_id", "number", "name", "age").
			Where("user_id >= ?", sql.NullInt64{Int64: 5, Valid: true}).
			Where("user_id < ?", sql.NullInt64{Int64: 10, Valid: true}).
			Asc("user_id").
			Rows(&Users{})

		if err != nil {
			return nil, err
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		return rows, nil
	})
	assert.NoError(t, err)

	rows, ok := result.(*xorm.Rows)
	assert.True(t, ok)
	assert.NotNil(t, rows)
	defer rows.Close()

	for i := 5; rows.Next(); i++ {
		var (
			userID int64
			number string
			name   string
			age    uint32
		)
		assert.NoError(t, rows.Scan(&userID, &number, &name, &age))
		assert.Equal(t, users[i].UserID.Int64, userID)
		assert.Equal(t, users[i].Number, number)
		assert.Equal(t, users[i].Name, name)
		assert.Equal(t, users[i].Age, age)
	}
}

func TestRowsRawYQL(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	users := getUsersData()

	session := engine.NewSession()
	defer session.Close()

	_, err = session.Insert(users)
	assert.NoError(t, err)

	rows, err := session.SQL(`
		SELECT user_id, number, age, created_at FROM users
		ORDER BY created_at DESC, age ASC
		LIMIT 10 OFFSET 5;
	`).
		Rows(&Users{})
	assert.NoError(t, err)
	assert.NotNil(t, rows)

	defer rows.Close()

	for rows.Next() {
		var user Users
		assert.NoError(t, rows.Scan(&user))
	}
}
