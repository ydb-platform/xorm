package ydb

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestInsertOne(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	user := Users{
		Name: "Dat",
		Age:  21,
		Account: Account{
			UserID: sql.NullInt64{Int64: 1234, Valid: true},
			Number: "56789",
		},
	}

	_, err = engine.InsertOne(&user)
	assert.NoError(t, err)

	has, err := engine.Exist(&user)
	assert.NoError(t, err)
	assert.True(t, has)
}

func TestInsertMultiStruct(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	users := getUsersData()

	_, err = engine.Insert(&users)
	assert.NoError(t, err)

	cnt, err := engine.Count(&Users{})
	assert.NoError(t, err)
	assert.Equal(t, int64(len(users)), cnt)
}

func TestInsertCreated(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	curTime := time.Now()
	users := getUsersData()

	_, err = engine.Insert(&users)
	assert.NoError(t, err)

	err = engine.Table(&Users{}).Cols("created_at").Find(&users)
	assert.NoError(t, err)

	loc := engine.GetTZLocation()
	for _, user := range users {
		layout := "2006-01-02 15:04:05"
		assert.EqualValues(t, curTime.In(loc).Format(layout), user.Created.In(loc).Format(layout))
	}
}

func TestInsertMapInterface(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	user := map[string]interface{}{
		"name":    "Dat",
		"age":     uint32(22),
		"user_id": sql.NullInt64{Int64: int64(1), Valid: true},
		"number":  uuid.NewString(),
	}

	_, err = engine.Table("users").Insert(user)
	assert.NoError(t, err)

	res := Users{
		Account: Account{
			UserID: user["user_id"].(sql.NullInt64),
			Number: user["number"].(string),
		},
	}
	has, err := engine.Get(&res)
	assert.NoError(t, err)
	assert.True(t, has)

	assert.Equal(t, res.Name, user["name"])
	assert.Equal(t, res.Age, user["age"])
	assert.Equal(t, res.UserID, user["user_id"])
	assert.Equal(t, res.Number, user["number"])
}

func TestInsertMultiMapInterface(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	users := []map[string]interface{}{}

	for i := 0; i < 20; i++ {
		users = append(users, map[string]interface{}{
			"name":    fmt.Sprintf("Dat - %d", i),
			"age":     uint32(22 + i),
			"user_id": sql.NullInt64{Int64: int64(i + 1), Valid: true},
			"number":  uuid.NewString(),
		})
	}

	_, err = engine.Table("users").Insert(users)
	assert.NoError(t, err)

	cnt, err := engine.Table("users").Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(len(users)), cnt)
}

func TestInsertCustomType(t *testing.T) {
	type RowID = uint64

	type Row struct {
		ID               RowID      `xorm:"pk 'id'"`
		PayloadStr       *string    `xorm:"'payload_str'"`
		PayloadDouble    *float64   `xorm:"'payload_double'"`
		PayloadTimestamp *time.Time `xorm:"'payload_timestamp'"`
	}

	rows := make([]Row, 0)
	for i := 0; i < 10; i++ {
		rows = append(rows, Row{
			ID:               RowID(i),
			PayloadStr:       func(s string) *string { return &s }(fmt.Sprintf("payload#%d", i)),
			PayloadDouble:    func(f float64) *float64 { return &f }((float64)(i)),
			PayloadTimestamp: func(t time.Time) *time.Time { return &t }(time.Now()),
		})
	}

	assert.NoError(t, PrepareScheme(&Row{}))
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)

	session := engine.NewSession()
	defer session.Close()

	_, err = session.Insert(&rows)
	assert.NoError(t, err)

	cnt, err := session.Count(&Row{})
	assert.NoError(t, err)
	assert.EqualValues(t, 10, cnt)
}
