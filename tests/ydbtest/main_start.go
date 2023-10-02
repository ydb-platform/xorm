package ydb

import (
	"context"
	"flag"
	"log"
	"testing"

	"xorm.io/xorm/schemas"
)

var (
	enginePool *EngineWithMode
	connString string
)

var (
	db             = flag.String("db", "sqlite3", "the tested database")
	showSQL        = flag.Bool("show_sql", true, "show generated SQLs")
	ptrConnStr     = flag.String("conn_str", "./test.db?cache=shared&mode=rwc", "test database connection string")
	cacheFlag      = flag.Bool("cache", false, "if enable cache")
	quotePolicyStr = flag.String("quote", "always", "quote could be always, none, reversed")
)

func MainTest(m *testing.M) int {
	flag.Parse()

	if db == nil || *db != string(schemas.YDB) {
		log.Println("this tests only apply for ydb")
		return -1
	}

	if ptrConnStr == nil {
		log.Println("you should indicate conn string")
		return -1
	}
	connString = *ptrConnStr

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	enginePool = NewEngineWithMode(ctx, connString)
	defer func() {
		_ = enginePool.Close()
	}()

	code := m.Run()
	defer func(code int) {
		log.Println("> Clean up")
		_ = CleanUp()
	}(code)

	return code
}