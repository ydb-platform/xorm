package ydb

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	xormLog "xorm.io/xorm/log"

	_ "github.com/ydb-platform/ydb-go-sdk/v3"

	"xorm.io/xorm"
)

type QueryMode int

const (
	DataQueryMode = QueryMode(iota)
	ExplainQueryMode
	ScanQueryMode
	SchemeQueryMode
	ScriptingQueryMode

	DefaultQueryMode = DataQueryMode
)

func (mode QueryMode) String() string {
	switch mode {
	case DataQueryMode:
		return "data"
	case ScanQueryMode:
		return "scan"
	case ExplainQueryMode:
		return "explain"
	case SchemeQueryMode:
		return "scheme"
	case ScriptingQueryMode:
		return "scripting"
	default:
		return "data"
	}
}

type EngineWithMode struct {
	engineCached map[QueryMode]*xorm.Engine
	dsn          string
	ctx          context.Context
}

func NewEngineWithMode(ctx context.Context, dsn string) *EngineWithMode {
	return &EngineWithMode{
		ctx:          ctx,
		dsn:          dsn,
		engineCached: make(map[QueryMode]*xorm.Engine),
	}
}

func createEngine(dsn string) (*xorm.Engine, error) {
	log.Printf("> connect: %s\n", dsn)
	return xorm.NewEngine(*db, dsn)
}

func constructDSN(dsn string, query ...string) string {
	info, err := url.Parse(dsn)
	if err != nil {
		panic(fmt.Errorf("failed to parse dsn: %s", dsn))
	}

	if info.RawQuery != "" {
		dsn = strings.Join(append([]string{dsn}, query...), "&")
	} else {
		q := strings.Join(query, "&")
		dsn = dsn + "?" + q
	}

	return dsn
}

func (em *EngineWithMode) getEngine(queryMode QueryMode) (*xorm.Engine, error) {
	if e, has := em.engineCached[queryMode]; has {
		if e.PingContext(em.ctx) == nil {
			return em.engineCached[queryMode], nil
		}
	}

	dsn := constructDSN(em.dsn, fmt.Sprintf("go_query_mode=%s", queryMode))
	engine, err := createEngine(dsn)
	if err != nil {
		return nil, err
	}

	engine.ShowSQL(*showSQL)
	engine.SetLogLevel(xormLog.LOG_DEBUG)

	appLoc, _ := time.LoadLocation("America/New_York")
	DbLoc, _ := time.LoadLocation("Europe/Moscow")
	engine.SetTZLocation(appLoc)
	engine.SetTZDatabase(DbLoc)

	engine.SetDefaultContext(em.ctx)

	engine.SetMaxOpenConns(50)
	engine.SetMaxIdleConns(50)
	engine.SetConnMaxIdleTime(time.Second)
	engine.EnableSessionID(true)

	em.engineCached[queryMode] = engine
	return em.engineCached[queryMode], nil
}

func (em *EngineWithMode) Close() error {
	for mode, engine := range em.engineCached {
		if err := engine.Close(); err != nil {
			return err
		}
		log.Printf("> close engine: %s\n", mode)
		delete(em.engineCached, mode)
	}
	return nil
}

func (em *EngineWithMode) GetDefaultEngine() (*xorm.Engine, error) {
	return em.getEngine(DefaultQueryMode)
}

func (em *EngineWithMode) GetDataQueryEngine() (*xorm.Engine, error) {
	return em.getEngine(DataQueryMode)
}

func (em *EngineWithMode) GetScanQueryEngine() (*xorm.Engine, error) {
	return em.getEngine(ScanQueryMode)
}

func (em *EngineWithMode) GetExplainQueryEngine() (*xorm.Engine, error) {
	return em.getEngine(ExplainQueryMode)
}

func (em *EngineWithMode) GetSchemeQueryEngine() (*xorm.Engine, error) {
	return em.getEngine(SchemeQueryMode)
}

func (em *EngineWithMode) GetScriptQueryEngine() (*xorm.Engine, error) {
	return em.getEngine(ScriptingQueryMode)
}

func PrepareScheme(bean ...interface{}) error {
	engine, err := enginePool.GetSchemeQueryEngine()
	if err != nil {
		return err
	}

	if err := engine.DropTables(bean...); err != nil {
		return err
	}

	if err := engine.CreateTables(bean...); err != nil {
		return err
	}

	return nil
}

func CleanUp() error {
	engine, err := enginePool.GetSchemeQueryEngine()
	if err != nil {
		return err
	}

	tables, err := engine.Dialect().GetTables(engine.DB(), enginePool.ctx)
	if err != nil {
		return err
	}

	beans := make([]interface{}, 0)
	for _, table := range tables {
		beans = append(beans, table.Name)
	}

	err = engine.DropTables(beans...)
	return err
}
