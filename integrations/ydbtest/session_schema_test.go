package ydb

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"xorm.io/xorm"
	"xorm.io/xorm/schemas"
)

func TestCreateTable(t *testing.T) {
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	assert.NoError(t, session.DropTable(&Users{}))
	assert.NoError(t, session.CreateTable(&Users{}))
	assert.NoError(t, session.CreateTable(&Users{}))
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

	for _, tblNames := range []string{"users", "series", "seasons", "episodes"} {
		assert.NoError(t, session.DropTable(tblNames))
	}

	assert.NoError(t, session.CreateTable(&Users{}))
	assert.NoError(t, session.CreateTable(&Series{}))
	assert.NoError(t, session.CreateTable(&Seasons{}))
	assert.NoError(t, session.CreateTable(&Episodes{}))
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

	engine, err := enginePool.GetDataQueryEngine()
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
	assert.NoError(t, PrepareScheme(&Account{}))
	assert.NoError(t, PrepareScheme(&Series{}))
	assert.NoError(t, PrepareScheme(&Seasons{}))
	assert.NoError(t, PrepareScheme(&Episodes{}))
	assert.NoError(t, PrepareScheme(&TestEpisodes{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	tables, err := engine.Dialect().GetTables(engine.DB(), enginePool.ctx)
	assert.NoError(t, err)

	expected := []string{
		"/local/users",
		"/local/account",
		"/local/series",
		"/local/seasons",
		"/local/episodes",
		"/local/test/episodes",
	}

	tableNames := []string{}
	for _, table := range tables {
		tableNames = append(tableNames, table.Name)
	}

	for _, e := range expected {
		assert.Contains(t, tableNames, e)
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

func TestGetColumns(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	dialect := engine.Dialect()
	cols, colsMap, err := dialect.GetColumns(engine.DB(), enginePool.ctx, (&Users{}).TableName())
	assert.NoError(t, err)
	assert.NotNil(t, cols)

	expectedCols := []string{"name", "age", "user_id", "number", "created_at", "updated_at"}
	assert.ElementsMatch(t, expectedCols, cols)

	expectedType := []string{"VARCHAR", "UNSIGNED MEDIUMINT", "BIGINT", "VARCHAR", "TIMESTAMP", "TIMESTAMP"}
	for i, col := range expectedCols {
		assert.Equal(t, expectedType[i], colsMap[col].SQLType.Name)
	}
}

func TestSyncNewTable(t *testing.T) {
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	assert.NoError(t, session.DropTable(&Users{}))
	assert.NoError(t, session.DropTable(&Account{}))
	assert.NoError(t, session.DropTable(&Series{}))
	assert.NoError(t, session.DropTable(&Seasons{}))
	assert.NoError(t, session.DropTable(&Episodes{}))
	assert.NoError(t, session.DropTable(&TestEpisodes{}))

	assert.NoError(t, session.Sync(
		&Users{},
		&Account{},
		&Series{},
		&Seasons{},
		&Episodes{},
		&TestEpisodes{}))
}

func TestSyncOldTable(t *testing.T) {
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	assert.NoError(t, PrepareScheme(&Users{}))
	assert.NoError(t, PrepareScheme(&Account{}))
	assert.NoError(t, PrepareScheme(&Series{}))
	assert.NoError(t, PrepareScheme(&Seasons{}))
	assert.NoError(t, PrepareScheme(&Episodes{}))
	assert.NoError(t, PrepareScheme(&TestEpisodes{}))

	assert.NoError(t, session.Sync(
		&Users{},
		&Account{},
		&Series{}))

	assert.NoError(t, session.Sync(
		&Seasons{},
		&Episodes{},
		&TestEpisodes{}))
}

type oriIndexSync struct {
	Uuid int64 `xorm:"pk"`

	A int64 `xorm:"index(idx_a)"`
	B int64 `xorm:"index(idx_a)"`
	C int64 `xorm:"index(idx_a)"`

	D int64 `xorm:"index(idx_b)"`
	E int64 `xorm:"index(idx_b)"`
	F int64 `xorm:"index(idx_b)"`

	G int64 `xorm:"index(idx_c)"`

	H int64
	I int64
}

func (*oriIndexSync) TableName() string {
	return "test_sync_index"
}

type newIndexSync struct {
	Uuid int64 `xorm:"pk"`

	A int64
	B int64 `xorm:"index(idx_a)"`
	C int64 `xorm:"index(idx_a)"`

	D int64 `xorm:"index(idx_b)"`
	E int64 `xorm:"index(idx_b)"`
	F int64 `xorm:"index(idx_b)"`

	G int64

	H int64 `xorm:"index(idx_c)"`
	I int64 `xorm:"index(idx_d)"`
}

func (*newIndexSync) TableName() string {
	return "test_sync_index"
}

func TestIndexSync(t *testing.T) {
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	assert.NoError(t, engine.Sync(&oriIndexSync{}, &newIndexSync{}))

	dialect := engine.Dialect()
	index, err := dialect.GetIndexes(engine.DB(), enginePool.ctx, "test_sync_index")
	assert.NoError(t, err)

	assert.NotNil(t, index["idx_a"])
	assert.ElementsMatch(t, []string{"b", "c"}, index["idx_a"].Cols)

	assert.NotNil(t, index["idx_b"])
	assert.ElementsMatch(t, []string{"d", "e", "f"}, index["idx_b"].Cols)

	assert.NotNil(t, index["idx_c"])
	assert.ElementsMatch(t, []string{"h"}, index["idx_c"].Cols)

	assert.NotNil(t, index["idx_d"])
	assert.ElementsMatch(t, []string{"i"}, index["idx_d"].Cols)

	assert.NoError(t, engine.Sync(&newIndexSync{}, &oriIndexSync{}))
	index, err = dialect.GetIndexes(engine.DB(), enginePool.ctx, "test_sync_index")
	assert.NoError(t, err)

	assert.NotNil(t, index["idx_a"])
	assert.ElementsMatch(t, []string{"a", "b", "c"}, index["idx_a"].Cols)

	assert.NotNil(t, index["idx_b"])
	assert.ElementsMatch(t, []string{"d", "e", "f"}, index["idx_b"].Cols)

	assert.NotNil(t, index["idx_c"])
	assert.ElementsMatch(t, []string{"g"}, index["idx_c"].Cols)

	assert.Nil(t, index["idx_d"])
}

type oriCols struct {
	Uuid    int64 `xorm:"pk"`
	A       int64
	B       int64
	C       int64
	D       int64
	NewType int64
}

func (*oriCols) TableName() string {
	return "test_sync_cols"
}

type newCols struct {
	Uuid    int64 `xorm:"pk"`
	A       int64
	B       int64
	C       int64
	D       int64
	E       int64
	F       int64
	G       int64
	NewType string
}

func (*newCols) TableName() string {
	return "test_sync_cols"
}

func TestSyncCols(t *testing.T) {
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	assert.NoError(t, engine.Sync(&oriCols{}, &newCols{}))

	dialect := engine.Dialect()
	cols, colMaps, err := dialect.GetColumns(engine.DB(), enginePool.ctx, "test_sync_cols")
	assert.NoError(t, err)
	assert.NotNil(t, colMaps)
	assert.ElementsMatch(t, []string{"uuid", "a", "b", "c", "d", "e", "f", "g", "new_type"}, cols)
	assert.EqualValues(t, schemas.BigInt, colMaps["new_type"].SQLType.Name)
}

type syncA struct {
	Uuid    int64 `xorm:"pk"`
	A       int64 `xorm:"index(idx_a)"`
	B       int64 `xorm:"index(idx_b)"`
	C       int64 `xorm:"index(idx_c)"`
	D       int64
	NewType int64
}

func (*syncA) TableName() string {
	return "test_overall_sync"
}

type syncB struct {
	Uuid    int64 `xorm:"pk"`
	A       int64 `xorm:"index(idx_a)"`  // common index
	B       int64 `xorm:"index(idx_bb)"` // common index but keep old name: `idx_b``
	C       int64
	D       int64 `xorm:"index(idx_c)"`
	E       int64 `xorm:"index(idx_d)"`
	F       int64 `xorm:"index(idx_e)"`
	G       int64
	NewType string `xorm:"index(idx_f)"`
}

func (*syncB) TableName() string {
	return "test_overall_sync"
}

func TestSyncOverall(t *testing.T) {
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	assert.NoError(t, engine.Sync(&syncA{}, &syncB{}))

	dialect := engine.Dialect()
	cols, colMaps, err := dialect.GetColumns(engine.DB(), enginePool.ctx, (&syncA{}).TableName())
	assert.NoError(t, err)
	assert.NotNil(t, colMaps)
	assert.ElementsMatch(t, []string{"uuid", "a", "b", "c", "d", "e", "f", "g", "new_type"}, cols)
	assert.EqualValues(t, schemas.BigInt, colMaps["new_type"].SQLType.Name)

	indexesMap, err := dialect.GetIndexes(engine.DB(), enginePool.ctx, (&syncB{}).TableName())
	assert.NoError(t, err)
	assert.NotNil(t, indexesMap)

	assert.NotNil(t, indexesMap["idx_a"])
	assert.ElementsMatch(t, []string{"a"}, indexesMap["idx_a"].Cols)

	assert.NotNil(t, indexesMap["idx_b"])
	assert.ElementsMatch(t, []string{"b"}, indexesMap["idx_b"].Cols)

	assert.NotNil(t, indexesMap["idx_c"])
	assert.ElementsMatch(t, []string{"d"}, indexesMap["idx_c"].Cols)

	assert.NotNil(t, indexesMap["idx_d"])
	assert.ElementsMatch(t, []string{"e"}, indexesMap["idx_d"].Cols)

	assert.NotNil(t, indexesMap["idx_e"])
	assert.ElementsMatch(t, []string{"f"}, indexesMap["idx_e"].Cols)

	assert.NotNil(t, indexesMap["idx_f"])
	assert.ElementsMatch(t, []string{"new_type"}, indexesMap["idx_f"].Cols)
}

func TestDBMetas(t *testing.T) {
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)

	dialect := engine.Dialect()

	_, err = engine.TransactionContext(enginePool.ctx, func(ctx context.Context, session *xorm.Session) (interface{}, error) {
		assert.NoError(t, session.Sync(&Users{}))

		exist, err := dialect.IsTableExist(session.Tx(), ctx, (&Users{}).TableName())
		assert.NoError(t, err)
		if err != nil {
			return nil, err
		}
		assert.True(t, exist)

		tables, err := dialect.GetTables(session.Tx(), ctx)
		assert.NoError(t, err)
		assert.NotNil(t, tables)
		ok := false
		for _, table := range tables {
			if path.Join(dialect.URI().DBName, (&Users{}).TableName()) == table.Name {
				ok = true
				break
			}
		}
		assert.True(t, ok)
		return nil, nil
	})
	assert.NoError(t, err)
}

/* func TestErr(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	conn, err := engine.DB().Conn(enginePool.ctx)
	assert.NoError(t, err)

	table := path.Join(engine.Dialect().URI().DBName, (&Users{}).TableName())
	err = conn.Raw(func(dc interface{}) error {
		q, ok := dc.(interface {
			GetTables(context.Context, string) ([]string, error)
		})
		if !ok {
			return fmt.Errorf("driver does not supported query for metadata")
		}
		tbl, err := q.GetTables(enginePool.ctx, table)
		if err != nil {
			return err
		}
		log.Println("got:", tbl)
		return nil
	})
	assert.NoError(t, err)
}
*/
