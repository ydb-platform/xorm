package ydb

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"xorm.io/xorm/schemas"
)

func TestPing(t *testing.T) {
	engine, err := enginePool.GetDefaultEngine()
	assert.NoError(t, err)

	assert.NoError(t, engine.Ping())
}

func TestPingContext(t *testing.T) {
	engine, err := enginePool.GetDefaultEngine()
	assert.NoError(t, err)

	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancelFunc()

	time.Sleep(time.Nanosecond)

	err = engine.PingContext(ctx)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "context deadline exceeded")
	}
}

var dbtypes = []schemas.DBType{schemas.SQLITE, schemas.MYSQL, schemas.POSTGRES, schemas.MSSQL, schemas.YDB}

func TestDump(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	users := getUsersData()

	_, err = engine.Insert(&users)
	assert.NoError(t, err)

	err = os.MkdirAll(".dump", fs.ModeDir|fs.ModePerm)
	assert.NoError(t, err)

	fp := fmt.Sprintf(".dump/%v.sql", engine.Dialect().URI().DBType)
	_, _ = os.Create(fp)
	assert.NoError(t, engine.DumpAllToFile(fp))

	for _, tp := range dbtypes {
		name := fmt.Sprintf(".dump/dump_%v.sql", tp)
		t.Run(name, func(t *testing.T) {
			assert.NoError(t, engine.DumpAllToFile(name, tp))
		})
	}
}

func TestDumpTables(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	users := getUsersData()

	_, err = engine.Insert(&users)
	assert.NoError(t, err)

	fp := fmt.Sprintf(".dump/%v-table.sql", engine.Dialect().URI().DBType)
	_, _ = os.Create(fp)
	tb, err := engine.TableInfo(new(Users))
	assert.NoError(t, err)
	assert.NoError(t, engine.DumpTablesToFile([]*schemas.Table{tb}, fp))

	for _, tp := range dbtypes {
		name := fmt.Sprintf(".dump/dump_%v-table.sql", tp)
		_, _ = os.Create(name)
		t.Run(name, func(t *testing.T) {
			assert.NoError(t, engine.DumpTablesToFile([]*schemas.Table{tb}, name, tp))
		})
	}
}

func TestImportDDL(t *testing.T) {
	engine, err := enginePool.GetSchemeQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	_, err = session.ImportFile("./testdata/DDL.sql")
	assert.NoError(t, err)
}

func TestImportDML(t *testing.T) {
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()
	defer session.Rollback()

	assert.NoError(t, session.Begin())

	_, err = session.ImportFile("./testdata/DML.sql")
	assert.NoError(t, err)

	assert.NoError(t, session.Commit())
}
