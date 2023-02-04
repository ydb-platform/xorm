package ydb

import (
	"context"
	"flag"
	"log"
	"testing"

	"xorm.io/xorm"
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

func MainTest(m *testing.M) int {
	flag.Parse()

	dbType = *db
	if dbType != string(schemas.YDB) {
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

	enginePool = &EngineWithMode{
		engineCached: make(map[string]*xorm.Engine),
		dsn:          connString,
		ctx:          ctx,
	}
	defer func() {
		_ = enginePool.Close()
	}()

	log.Println("testing", dbType, connString)

	code := m.Run()
	defer func(code int) {
		log.Println("Finished Testing >>> Cleaning up...")
		_ = CleanUp()
	}(code)

	defer func() {
		for benchmarkName, benchmarkF := range map[string]func(b *testing.B){
			"BenchmarkSync": BenchmarkSync,
		} {
			log.Println(benchmarkName)
			res := testing.Benchmark(benchmarkF)
			log.Printf("%+v\n", res)
		}
	}()

	return code
}
