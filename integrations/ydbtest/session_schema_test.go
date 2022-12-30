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

func TestIsColumnExist(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	dialect := engine.Dialect()
	cols := []string{"name", "age", "user_id", "number", "created_at", "updated_at"}

	for _, col := range cols {
		exist, err := dialect.IsColumnExist(engine.DB(), enginePool.ctx, (&Users{}).TableName(), col)
		assert.NoError(t, err)
		assert.True(t, exist)
	}

	cols = []string{"name_", "age_", "user_id_", "number_", "created_at_", "updated_at_"}
	for _, col := range cols {
		exist, err := dialect.IsColumnExist(engine.DB(), enginePool.ctx, (&Users{}).TableName(), col)
		assert.NoError(t, err)
		assert.False(t, exist)
	}
}

func TestGetTables(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	tables, err := engine.Dialect().GetTables(engine.DB(), enginePool.ctx)
	assert.NoError(t, err)

	expected := []string{
		"/local/users",
		"/local/series",
		"/local/episodes",
		"/local/seasons",
		"/local/userinfo",
		"/local/check_list",
		"/local/condition",
		"/local/test/episodes", // REMOVE
	}

	found := make(map[string]bool)
	for _, table := range tables {
		found[table.Name] = true
	}

	for _, e := range expected {
		assert.True(t, found[e])
	}
}

func TestGetIndexes(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Seasons{}))
	assert.NoError(t, PrepareScheme(&Series{}))
	assert.NoError(t, PrepareScheme(&Episodes{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	dialect := engine.Dialect()
	index, err := dialect.GetIndexes(engine.DB(), enginePool.ctx, (&Series{}).TableName())
	assert.NoError(t, err)
	assert.NotNil(t, index["index_series_title"])
	assert.EqualValues(t, index["index_series_title"].Cols, []string{"title"})

	index, err = dialect.GetIndexes(engine.DB(), enginePool.ctx, (&Seasons{}).TableName())
	assert.NoError(t, err)
	assert.NotNil(t, index["index_series_title"])
	assert.EqualValues(t, index["index_series_title"].Cols, []string{"title"})
	assert.NotNil(t, index["index_season_first_aired"])
	assert.EqualValues(t, index["index_season_first_aired"].Cols, []string{"first_aired"})

	index, err = dialect.GetIndexes(engine.DB(), enginePool.ctx, (&Episodes{}).TableName())
	assert.NoError(t, err)
	assert.NotNil(t, index["index_episodes_air_date"])
	assert.EqualValues(t, index["index_episodes_air_date"].Cols, []string{"air_date"})

	type TestIndex struct {
		Uuid   int64 `xorm:"pk"`
		IndexA int64 `xorm:"index(a)"`
		IndexB int64 `xorm:"index(a)"`

		IndexC int64 `xorm:"index(b)"`
		IndexD int64 `xorm:"index(b)"`
		IndexE int64 `xorm:"index(b)"`

		IndexF int64 `xorm:"index(c)"`
		IndexG int64 `xorm:"index(c)"`
		IndexH int64 `xorm:"index(c)"`
		IndexI int64 `xorm:"index(c)"`
	}
	assert.NoError(t, PrepareScheme(&TestIndex{}))

	index, err = dialect.GetIndexes(engine.DB(), enginePool.ctx, "test_index")
	assert.NoError(t, err)
	assert.NotNil(t, index["a"])
	assert.EqualValues(t, 2, len(index["a"].Cols))
	assert.ElementsMatch(t, []string{"index_a", "index_b"}, index["a"].Cols)

	assert.NotNil(t, index["b"])
	assert.EqualValues(t, 3, len(index["b"].Cols))
	assert.ElementsMatch(t, []string{"index_c", "index_d", "index_e"}, index["b"].Cols)

	assert.NotNil(t, index["c"])
	assert.EqualValues(t, 4, len(index["c"].Cols))
	assert.ElementsMatch(t, []string{"index_f", "index_g", "index_h", "index_i"}, index["c"].Cols)
}

// TODO: sync test
