package ydb

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"xorm.io/xorm"
	"xorm.io/xorm/retry"
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
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
}

var dbtypes = []schemas.DBType{schemas.SQLITE, schemas.MYSQL, schemas.POSTGRES, schemas.MSSQL, schemas.ORACLE, schemas.YDB}

func TestDump(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	users := getUsersData()
	_, err = engine.Insert(&users)
	assert.NoError(t, err)

	for _, tp := range dbtypes {
		name := fmt.Sprintf("%s/dump_%v-all.sql", t.TempDir(), tp)
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

	tb, err := engine.TableInfo(new(Users))
	assert.NoError(t, err)

	for _, tp := range dbtypes {
		name := fmt.Sprintf("%s/dump_%v-table.sql", t.TempDir(), tp)
		t.Run(name, func(t *testing.T) {
			assert.NoError(t, engine.DumpTablesToFile([]*schemas.Table{tb}, name, tp))
		})
	}
}

func TestImportFromDumpFile(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	assert.NoError(t, PrepareScheme(&Series{}))
	assert.NoError(t, PrepareScheme(&Episodes{}))
	assert.NoError(t, PrepareScheme(&Seasons{}))

	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	t.Run("dump-and-import", func(t *testing.T) {
		name := fmt.Sprintf("%s/dump_%v-all-tables.yql", t.TempDir(), schemas.YDB)
		assert.NoError(t, engine.DumpAllToFile(name, schemas.YDB))

		err = engine.DropTables(&Users{}, &Series{}, &Seasons{}, &Episodes{})
		assert.NoError(t, err)

		_, err = engine.ImportFile(name)
		assert.NoError(t, err)
	})

	t.Run("insert-data", func(t *testing.T) {
		users := getUsersData()
		seriesData, seasonsData, episodesData := getData()

		_, err = engine.Insert(&seriesData, &seasonsData, &episodesData, &users)
		assert.NoError(t, err)
	})
}

func TestImportDDL(t *testing.T) {
	engine, err := enginePool.GetSchemeQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	_, err = session.ImportFile("testdata/DDL.yql")
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

	_, err = session.ImportFile("testdata/DML.yql")
	assert.NoError(t, err)

	assert.NoError(t, session.Commit())
}

func TestDBVersion(t *testing.T) {
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	version, err := engine.DBVersion()
	assert.NoError(t, err)
	t.Log(version.Edition + " " + version.Number)
}

func TestRetry(t *testing.T) {
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	err = engine.Do(enginePool.ctx, func(ctx context.Context, session *xorm.Session) (err error) {
		return session.DropTable(&Users{})
	}, retry.WithID("retry-test-drop-table"),
		retry.WithIdempotent(true))
	assert.NoError(t, err)

	err = engine.Do(enginePool.ctx, func(ctx context.Context, session *xorm.Session) (err error) {
		return session.CreateTable(&Users{})
	}, retry.WithID("retry-test-create-table"),
		retry.WithIdempotent(true))
	assert.NoError(t, err)

	users := getUsersData()
	err = engine.Do(enginePool.ctx, func(ctx context.Context, session *xorm.Session) (err error) {
		_, err = session.Insert(users)
		return err
	}, retry.WithID("retry-test-insert"),
		retry.WithIdempotent(true))
	assert.NoError(t, err)
}

func TestRetryTx(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Seasons{}))
	assert.NoError(t, PrepareScheme(&Series{}))
	assert.NoError(t, PrepareScheme(&Episodes{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	series, seasons, episodes := getData()
	err = engine.DoTx(enginePool.ctx, func(ctx context.Context, session *xorm.Session) (err error) {
		_, err = session.Insert(&series)
		if err != nil {
			return err
		}
		_, err = session.Insert(&seasons)
		if err != nil {
			return err
		}

		_, err = session.Insert(&episodes)
		if err != nil {
			return err
		}
		return nil
	}, retry.WithID("retry-test-insert-tx"))
	assert.NoError(t, err)

	var seriesCnt, seasonsCnt, episodesCnt int64 = 0, 0, 0
	err = engine.DoTx(enginePool.ctx, func(ctx context.Context, session *xorm.Session) (err error) {
		seriesCnt, err = engine.Table(&Series{}).Count()
		if err != nil {
			return err
		}

		seasonsCnt, err = engine.Table(&Seasons{}).Count()
		if err != nil {
			return err
		}

		episodesCnt, err = engine.Table(&Episodes{}).Count()
		if err != nil {
			return err
		}
		return nil
	}, retry.WithIdempotent(true))

	assert.NoError(t, err)
	assert.EqualValues(t, seasonsCnt, len(seasons))
	assert.EqualValues(t, seriesCnt, len(series))
	assert.EqualValues(t, episodesCnt, len(episodes))
}
