package ydb

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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

	result, err := engine.Transaction(func(session *xorm.Session) (interface{}, error) {
		rows, err := session.
			Cols("user_id", "number", "name", "age").
			Where("user_id >= ?", sql.NullInt64{Int64: 5, Valid: true}).
			Where("user_id < ?", sql.NullInt64{Int64: 10, Valid: true}).
			Asc("user_id").
			Rows(&Users{})

		if err != nil {
			return nil, err
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

func TestRowsOverManyResultSet(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Series{}, &Seasons{}, &Episodes{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	series, seasons, episodes := getData()

	_, err = engine.Transaction(func(session *xorm.Session) (interface{}, error) {
		_, err := session.Insert(&series)
		if err != nil {
			return nil, err
		}
		_, err = session.Insert(&seasons)
		if err != nil {
			return nil, err
		}
		_, err = session.Insert(&episodes)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	assert.NoError(t, err)

	query := fmt.Sprintf("SELECT * FROM `%s`; SELECT * FROM `%s`; SELECT * FROM `%s`;",
		(&Series{}).TableName(),
		(&Seasons{}).TableName(),
		(&Episodes{}).TableName())

	rows, err := session.DB().QueryContext(enginePool.ctx, query)
	assert.NoError(t, err)

	expectedColumns := [][]string{
		{"series_id", "title", "series_info", "release_date", "comment"},
		{"series_id", "season_id", "title", "first_aired", "last_aired"},
		{"series_id", "season_id", "episode_id", "title", "air_date", "views"},
	}

	expectedTypes := [][]string{
		{"String", "Utf8", "Utf8", "Timestamp", "Utf8"},
		{"String", "String", "Utf8", "Timestamp", "Timestamp"},
		{"String", "String", "String", "Utf8", "Timestamp", "Uint64"},
	}

	for i := 0; rows.NextResultSet(); i++ {
		for rows.Next() {
			columns, err := rows.Columns()
			assert.NoError(t, err)
			assert.ElementsMatch(t, expectedColumns[i], columns)

			var types []string
			li, err := rows.ColumnTypes()
			assert.NoError(t, err)
			for _, val := range li {
				tp := val.DatabaseTypeName()
				if strings.HasPrefix(tp, "Optional") {
					tp = strings.TrimPrefix(tp, "Optional<")
					tp = strings.TrimSuffix(tp, ">")
				}
				types = append(types, tp)
			}
			assert.ElementsMatch(t, expectedTypes[i], types)
		}
	}
}
