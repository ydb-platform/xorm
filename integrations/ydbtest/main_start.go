package ydb

import (
	"flag"
	"fmt"
	"log"
	"testing"

	_ "github.com/ydb-platform/ydb-go-sdk/v3"

	"xorm.io/xorm"
	xormLog "xorm.io/xorm/log"
	"xorm.io/xorm/schemas"
)

var (
	enginePool *EngineWithMode
	dbType     string
	connString string

	db             = flag.String("db", "sqlite3", "the tested database")
	showSQL        = flag.Bool("show_sql", true, "show generated SQLs")
	ptrConnStr     = flag.String("conn_str", "./test.db?cache=shared&mode=rwc", "test database connection string")
	cacheFlag      = flag.Bool("cache", false, "if enable cache")
	quotePolicyStr = flag.String("quote", "always", "quote could be always, none, reversed")
)

type QueryMode int

type EngineWithMode struct {
	engineCached map[string]*xorm.Engine
	dsn          string
}

const (
	UnknownQueryMode = iota
	DataQueryMode
	ExplainQueryMode
	ScanQueryMode
	SchemeQueryMode
	ScriptingQueryMode

	DefaultQueryMode = DataQueryMode
)

var (
	typeToString = map[QueryMode]string{
		DataQueryMode:      "data",
		ScanQueryMode:      "scan",
		ExplainQueryMode:   "explain",
		SchemeQueryMode:    "scheme",
		ScriptingQueryMode: "scripting",
	}
)

func createEngine(dsn string) (*xorm.Engine, error) {
	log.Println("connect to", dsn)
	return xorm.NewEngine(dbType, dsn)
}

func (em *EngineWithMode) getEngine(queryMode QueryMode) (*xorm.Engine, error) {
	mode := typeToString[queryMode]

	if _, has := em.engineCached[mode]; has {
		return em.engineCached[mode], nil
	}

	engine, err := createEngine(fmt.Sprintf("%s?query_mode=%s", em.dsn, mode))
	if err != nil {
		return nil, err
	}

	engine.ShowSQL(*showSQL)
	engine.SetLogLevel(xormLog.LOG_DEBUG)

	em.engineCached[mode] = engine
	return em.engineCached[mode], nil
}

func (em *EngineWithMode) Close() error {
	var retErr error = nil
	for mode, engine := range em.engineCached {
		log.Println("Close", mode, "engine")
		if err := engine.Close(); err != nil {
			retErr = err
		}
	}
	return retErr
}

func GetDefaultEngine() (*xorm.Engine, error) {
	return enginePool.getEngine(DefaultQueryMode)
}

func GetScanQueryEngine() (*xorm.Engine, error) {
	return enginePool.getEngine(ScanQueryMode)
}

func GetExplainQueryEngine() (*xorm.Engine, error) {
	return enginePool.getEngine(ExplainQueryMode)
}

func GetSchemeQueryEngine() (*xorm.Engine, error) {
	return enginePool.getEngine(SchemeQueryMode)
}

func GetScriptQueryEngine() (*xorm.Engine, error) {
	return enginePool.getEngine(ScriptingQueryMode)
}

func MainTest(m *testing.M) int {
	flag.Parse()

	dbType = *db
	if dbType != string(schemas.YDB) {
		log.Println("this tests only apply for ydb")
		return 1
	}
	if ptrConnStr == nil {
		log.Println("you should indicate conn string")
		return 1
	}
	connString = *ptrConnStr

	enginePool = &EngineWithMode{
		engineCached: make(map[string]*xorm.Engine),
		dsn:          connString,
	}
	defer func() {
		_ = enginePool.Close()
	}()

	log.Println("testing", dbType, connString)
	code := m.Run()

	return code
}
