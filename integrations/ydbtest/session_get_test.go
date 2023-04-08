package ydb

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
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

	_, err = engine.InsertOne(&user)
	assert.NoError(t, err)

	var name string
	has, err := engine.Table("users").Cols("name").Get(&name)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.Equal(t, user.Name, name)

	var age uint64
	has, err = engine.Table("users").Cols("age").Get(&age)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.Equal(t, user.Age, uint32(age))

	var userId sql.NullInt64
	has, err = engine.Table("users").Cols("user_id").Get(&userId)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.Equal(t, user.UserID, userId)

	var number string
	has, err = engine.Table("users").Cols("number").Get(&number)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.Equal(t, user.Number, number)

	has, err = engine.
		Table("users").
		Cols("name", "age", "user_id", "number").
		Get(&name, &age, &userId, &number)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.Equal(t, user.Name, name)
	assert.Equal(t, user.Age, uint32(age))
	assert.Equal(t, user.UserID, userId)
	assert.Equal(t, user.Number, number)
}

func TestGetStruct(t *testing.T) {
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

	_, err = engine.InsertOne(&user)
	assert.NoError(t, err)

	var ret Users
	has, err := engine.Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.Equal(t, user.Name, ret.Name)
	assert.Equal(t, user.Age, ret.Age)
	assert.Equal(t, user.UserID, ret.UserID)
	assert.Equal(t, user.Number, ret.Number)

	_, err = engine.Delete(&user)
	assert.NoError(t, err)

	ret = Users{}
	has, err = engine.Where("user_id = ?", user.UserID).Get(&ret)
	assert.NoError(t, err)
	assert.False(t, has)
	assert.Equal(t, Users{}, ret)
}

func TestGetMap(t *testing.T) {
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

	_, err = engine.InsertOne(&user)
	assert.NoError(t, err)

	ret := make(map[string]string)
	has, err := engine.Table("users").Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)

	assert.Equal(t, 6, len(ret))
	assert.Equal(t, "datbeohbbh", ret["name"])
	assert.Equal(t, "22", ret["age"])
	assert.Equal(t, "22", ret["user_id"])
	assert.Equal(t, user.Number, ret["number"])
	assert.True(t, len(ret["created_at"]) > 0)
	assert.True(t, len(ret["updated_at"]) > 0)
}

func TestGetNullValue(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	user := Users{
		Account: Account{
			UserID: sql.NullInt64{Int64: 22, Valid: true},
			Number: uuid.NewString(),
		},
	}

	_, err = engine.InsertOne(&user)
	assert.NoError(t, err)

	var name string
	var age uint64
	has, err := engine.Table("users").Cols("name", "age").Get(&name, &age)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.Equal(t, "", name)
	assert.Equal(t, uint64(0), age)
}

func TestCustomTypes(t *testing.T) {
	type CustomInt int
	type CustomString string

	type TestCustomizeStruct struct {
		Uuid []byte `xorm:"pk"`
		Name CustomString
		Age  CustomInt
	}
	assert.NoError(t, PrepareScheme(&TestCustomizeStruct{}))

	data := TestCustomizeStruct{
		Uuid: []byte(uuid.NewString()),
		Name: "datbeohbbh",
		Age:  22,
	}

	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	defer func() {
		assert.NoError(t, session.DropTable(&TestCustomizeStruct{}))
	}()

	_, err = session.Insert(&data)
	assert.NoError(t, err)

	var name CustomString
	has, err := session.Table(&TestCustomizeStruct{}).Cols("name").Get(&name)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.Equal(t, CustomString("datbeohbbh"), name)

	var age CustomInt
	has, err = session.Table(&TestCustomizeStruct{}).Cols("age").Get(&age)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.Equal(t, CustomInt(22), age)
}

func TestGetTime(t *testing.T) {
	type GetTimeStruct struct {
		Uuid       int64 `xorm:"pk"`
		CreateTime time.Time
	}

	assert.NoError(t, PrepareScheme(&GetTimeStruct{}))

	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	defer func() {
		assert.NoError(t, session.DropTable(&GetTimeStruct{}))
	}()

	gts := GetTimeStruct{
		Uuid:       int64(1),
		CreateTime: time.Now().In(engine.GetTZLocation()),
	}
	_, err = session.Insert(&gts)
	assert.NoError(t, err)

	var gn time.Time
	has, err := session.Table(&GetTimeStruct{}).Cols("create_time").Get(&gn)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.EqualValues(t, gts.CreateTime.Format(time.RFC3339), gn.Format(time.RFC3339))
}
