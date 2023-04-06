package ydb

import (
	"testing"

	_ "github.com/lib/pq"
	_ "github.com/ydb-platform/ydb-go-sdk/v3"

	"xorm.io/xorm"

	"github.com/stretchr/testify/require"
)

func TestGenerator(t *testing.T) {
	dsn := "postgres://postgres:password@localhost:5430/dev-autoincr?sslmode=disable"
	g, err := xorm.NewGeneratorEngine("postgres", dsn)
	require.NoError(t, err)

	t.Run("init-id-table", func(t *testing.T) {
		err := g.CreateIdTable()
		require.NoError(t, err)
	})

	t.Run("gen-id", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			id, err := g.GenNextID()
			require.NoError(t, err)

			t.Logf("> next id = %d", id)
		}
	})
}

func TestYDBGenerator(t *testing.T) {
	connStr := "grpc://localhost:2136/local?go_query_bind=table_path_prefix(/local/test),numeric,declare" +
		"&dev_autoincr=postgres(postgres://postgres:password@localhost:5430/dev-autoincr?sslmode=disable)"

	engine, err := xorm.NewEngine("ydb", connStr)
	require.NoError(t, err)

	defer func() {
		_ = engine.Close()
	}()

	dialect := engine.Dialect()
	gen, ok := dialect.(interface {
		NextID() (uint64, error)
	})
	require.True(t, ok)

	for i := 0; i < 10; i++ {
		id, err := gen.NextID()
		require.NoError(t, err)

		t.Logf("> next id = %d", id)
	}
}

func TestYDBGeneratorCreateTable(t *testing.T) {
	connStr := "grpc://localhost:2136/local?query_mode=scripting&go_query_bind=table_path_prefix(/local/test),numeric,declare" +
		"&dev_autoincr=postgres(postgres://postgres:password@localhost:5430/dev-autoincr?sslmode=disable)"

	engine, err := xorm.NewEngine("ydb", connStr)
	engine.ShowSQL(true)
	require.NoError(t, err)

	defer func() {
		_ = engine.Close()
	}()

	type GenTable struct {
		A uint64 `xorm:"pk"`
		B uint64 `xorm:"'b'"`
		C uint64 `xorm:"'c' index(cc)"`
	}

	session := engine.NewSession()
	err = session.CreateTable(&GenTable{})
	require.NoError(t, err)

	err = session.DropTable(&GenTable{})
	require.NoError(t, err)
}
