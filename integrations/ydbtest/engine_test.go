package ydb

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
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

func TestDBVersion(t *testing.T) {
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	version, err := engine.DBVersion()
	assert.NoError(t, err)
	log.Println(version.Edition + " " + version.Number)
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

func _TestSimulationRetry(t *testing.T) {
	engine, err := enginePool.GetSchemeQueryEngine()
	assert.NoError(t, err)

	assert.NoError(t, engine.NewSession().DropTable(&Users{}))
	assert.NoError(t, engine.NewSession().DropTable(&Series{}))

	test := true
	err = engine.Do(enginePool.ctx, func(ctx context.Context, session *xorm.Session) (err error) {
		err = session.CreateTable(&Users{})
		if err != nil {
			return err
		}

		if test {
			log.Println("shut down ydb")
			time.Sleep(3 * time.Second)

			log.Println("turn on ydb")
			time.Sleep(3 * time.Second)

			log.Println("wait ydb")
			time.Sleep(2 * time.Second)

			test = false
		}

		err = session.CreateTable(&Series{})
		if err != nil {
			return err
		}
		return nil
	}, retry.WithID("retry-test-create-table"),
		retry.WithIdempotent(true),
		retry.WithBackoff(retry.NewBackoff(100*time.Millisecond, 1*time.Second, true)))
	assert.NoError(t, err)

	log.Println("no err:", err == nil)
	time.Sleep(10 * time.Second)
}

func _TestSimulationRetryTx(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))
	assert.NoError(t, PrepareScheme(&Seasons{}))
	assert.NoError(t, PrepareScheme(&Series{}))
	assert.NoError(t, PrepareScheme(&Episodes{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	series, seasons, _ := getData()
	pctx, cancel := context.WithTimeout(enginePool.ctx, time.Minute)
	defer cancel()
	test := true
	err = engine.DoTx(pctx, func(ctx context.Context, session *xorm.Session) (err error) {
		log.Println(engine.DB().Stats())
		_, err = session.Insert(&series)
		if err != nil {
			return err
		}

		if test {
			log.Println("shut down ydb")
			time.Sleep(3 * time.Second)

			log.Println("turn on ydb")
			time.Sleep(3 * time.Second)

			log.Println("wait ydb")
			time.Sleep(2 * time.Second)

			test = false
		}

		_, err = session.Insert(&seasons)
		if err != nil {
			return err
		}
		return nil
	}, retry.WithID("retry-test-insert-tx"),
		retry.WithIdempotent(true),
		retry.WithBackoff(retry.NewBackoff(100*time.Millisecond, 1*time.Second, true)),
		retry.WithMaxRetries(3))
	assert.NoError(t, err)

	log.Println("no err:", err == nil)
	time.Sleep(10 * time.Second)
}