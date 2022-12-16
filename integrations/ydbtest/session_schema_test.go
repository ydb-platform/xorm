package ydb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateTable(t *testing.T) {
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	if err := session.DropTable(&Users{}); err != nil {
		t.Fatal(err)
	}

	if err := session.CreateTable(&Users{}); err != nil {
		t.Fatal(err)
	}
}

func TestIsTableEmpty(t *testing.T) {
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()

	assert.NoError(t, session.DropTable(&Users{}))
	assert.NoError(t, session.CreateTable(&Users{}))

	session.Close()

	isEmpty, err := engine.IsTableEmpty(&Users{})
	assert.NoError(t, err)
	assert.True(t, isEmpty)

	tbName := engine.GetTableMapper().Obj2Table("users")
	isEmpty, err = engine.IsTableEmpty(tbName)
	assert.NoError(t, err)
	assert.True(t, isEmpty)
}

func TestCreateMultiTables(t *testing.T) {
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	for i := 0; i < 10; i++ {
		assert.NoError(t, session.DropTable("users"))
		assert.NoError(t, session.Table("users").CreateTable(&Users{}))
	}
}

func TestIsTableExists(t *testing.T) {
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	assert.NoError(t, session.DropTable(&Users{}))

	exist, err := session.IsTableExist(&Users{})
	assert.NoError(t, err)
	assert.False(t, exist)

	assert.NoError(t, session.CreateTable(&Users{}))

	exist, err = session.IsTableExist(&Users{})
	assert.NoError(t, err)
	assert.True(t, exist)
}

// TODO: sync test
