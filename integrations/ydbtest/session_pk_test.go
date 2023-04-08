package ydb

import (
	"database/sql"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"xorm.io/xorm/schemas"
)

func TestIntPK(t *testing.T) {
	type Int64PK struct {
		Uuid int64 `xorm:"pk"`
	}

	type Int32PK struct {
		Uuid int32 `xorm:"pk"`
	}

	assert.NoError(t, PrepareScheme(&Int64PK{}))
	assert.NoError(t, PrepareScheme(&Int32PK{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	for i := 0; i < 10; i++ {
		_, err = session.Insert(&Int64PK{Uuid: int64(i)})
		assert.NoError(t, err)

		_, err = session.Insert(&Int32PK{Uuid: int32(i)})
		assert.NoError(t, err)
	}

	var uuidsInt64 []int64
	err = session.
		Table(engine.GetTableMapper().Obj2Table("Int64PK")).
		Cols("uuid").
		Find(&uuidsInt64)
	assert.NoError(t, err)
	assert.EqualValues(t, 10, len(uuidsInt64))

	var uuidsInt32 []int32
	err = session.
		Table(engine.GetTableMapper().Obj2Table("Int32PK")).
		Cols("uuid").
		Find(&uuidsInt32)
	assert.NoError(t, err)
	assert.EqualValues(t, 10, len(uuidsInt32))

	for i := 0; i < 10; i++ {
		assert.Equal(t, int64(i), uuidsInt64[i])
		assert.Equal(t, int32(i), uuidsInt32[i])
	}
}

func TestUintPK(t *testing.T) {
	type Uint8PK struct {
		Uuid uint8 `xorm:"pk"`
	}

	type Uint32PK struct {
		Uuid uint32 `xorm:"pk"`
	}

	type Uint64PK struct {
		Uuid uint64 `xorm:"pk"`
	}

	assert.NoError(t, PrepareScheme(&Uint8PK{}))
	assert.NoError(t, PrepareScheme(&Uint32PK{}))
	assert.NoError(t, PrepareScheme(&Uint64PK{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	for i := 0; i < 10; i++ {
		_, err = session.Insert(&Uint8PK{Uuid: uint8(i)})
		assert.NoError(t, err)

		_, err = session.Insert(&Uint64PK{Uuid: uint64(i)})
		assert.NoError(t, err)

		_, err = session.Insert(&Uint32PK{Uuid: uint32(i)})
		assert.NoError(t, err)
	}

	var uuidsUint64 []uint64
	err = session.
		Table(engine.GetTableMapper().Obj2Table("Uint64PK")).
		Cols("uuid").
		Find(&uuidsUint64)
	assert.NoError(t, err)
	assert.EqualValues(t, 10, len(uuidsUint64))

	var uuidsUint32 []uint32
	err = session.
		Table(engine.GetTableMapper().Obj2Table("Uint32PK")).
		Cols("uuid").
		Find(&uuidsUint32)
	assert.NoError(t, err)
	assert.EqualValues(t, 10, len(uuidsUint32))

	var uuidsUint8 []uint8
	err = session.
		Table(engine.GetTableMapper().Obj2Table("Uint8PK")).
		Cols("uuid").
		Find(&uuidsUint8)
	assert.NoError(t, err)
	assert.EqualValues(t, 10, len(uuidsUint32))

	for i := 0; i < 10; i++ {
		assert.Equal(t, uint64(i), uuidsUint64[i])
		assert.Equal(t, uint32(i), uuidsUint32[i])
		assert.Equal(t, uint8(i), uuidsUint8[i])
	}
}

func TestStringPK(t *testing.T) {
	type CustomString string
	type StringPK struct {
		Uuid CustomString `xorm:"pk"`
	}

	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	tbName := engine.GetTableMapper().Obj2Table("StringPK")

	assert.NoError(t, engine.NewSession().DropTable(tbName))
	assert.NoError(t, engine.Sync(&StringPK{}))

	session := engine.NewSession()
	defer session.Close()

	for i := 0; i < 10; i++ {
		_, err = session.Insert(&StringPK{Uuid: CustomString(fmt.Sprintf("pk_%d", i))})
		assert.NoError(t, err)
	}

	id := rand.Int31n(10)
	var data StringPK
	has, err := session.ID(schemas.PK{fmt.Sprintf("pk_%d", id)}).Get(&data)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.EqualValues(t, fmt.Sprintf("pk_%d", id), data.Uuid)
}

func TestBytePK(t *testing.T) {
	type BytePK struct {
		Uuid []byte `xorm:"pk"`
	}

	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	tbName := engine.GetTableMapper().Obj2Table("BytePK")

	assert.NoError(t, engine.NewSession().DropTable(tbName))
	assert.NoError(t, engine.Sync(&BytePK{}))

	session := engine.NewSession()
	defer session.Close()

	for i := 0; i < 10; i++ {
		_, err = session.Insert(&BytePK{Uuid: []byte(fmt.Sprintf("pk_%d", i))})
		assert.NoError(t, err)
	}

	id := rand.Int31n(10)
	var data BytePK
	has, err := session.ID(schemas.PK{[]byte(fmt.Sprintf("pk_%d", id))}).Get(&data)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.EqualValues(t, []byte(fmt.Sprintf("pk_%d", id)), data.Uuid)
}

func TestCompositePK(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	users := getUsersData()

	session := engine.NewSession()
	defer session.Close()

	_, err = session.Insert(&users)
	assert.NoError(t, err)

	for i, user := range users {
		var data Users
		_, err = session.ID(schemas.PK{
			sql.NullInt64{Int64: int64(i), Valid: true},
			user.Number,
		}).Get(&data)
		assert.NoError(t, err)
		assert.Equal(t, user.Name, data.Name)
		assert.Equal(t, user.Age, data.Age)
		assert.Equal(t, user.UserID, data.UserID)
		assert.Equal(t, user.Number, data.Number)
	}
}
