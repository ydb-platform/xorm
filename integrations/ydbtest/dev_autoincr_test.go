package ydb

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"

	_ "github.com/ydb-platform/ydb-go-sdk/v3"

	"xorm.io/xorm"
)

func TestAutoIncr(t *testing.T) {
	type GenTable struct {
		A uint64 `xorm:"'a' pk autoincr"`
		B uint64 `xorm:"'b'"`
		C uint64 `xorm:"'c' index(cc)"`
	}

	// `declare,numeric` have different behavirour with `numeric,declare`
	connStr := "grpc://localhost:2136/local?query_mode=scripting&go_query_bind=table_path_prefix(/local/test),declare,numeric" +
		"&dev_autoincr=postgres(postgres://postgres:password@localhost:5430/dev-autoincr?sslmode=disable)"

	engine, err := xorm.NewEngine("ydb", connStr)
	engine.ShowSQL(true)
	assert.NoError(t, err)

	defer func() {
		session := engine.NewSession()
		session.DropTable(&GenTable{})

		_ = session.Close()
		_ = engine.Close()
	}()

	t.Run("prepare-scheme", func(t *testing.T) {
		session := engine.NewSession()
		defer func() {
			_ = session.Close()
		}()
		session.CreateTable(&GenTable{})
	})

	autoIncrID := int64(-1)

	t.Run("insert-struct", func(t *testing.T) {
		session := engine.NewSession()
		defer func() {
			_ = session.Close()
		}()
		for b := 1; b <= 3; b++ {
			for c := 1; c <= 3; c++ {
				x := GenTable{
					B: uint64(b),
					C: uint64(c),
				}
				_, err = session.Insert(&x)
				assert.NoError(t, err)

				if autoIncrID == -1 {
					autoIncrID = int64(x.A - 1)
				}

				autoIncrID += 1
				assert.EqualValues(t, autoIncrID, x.A)
			}
		}
	})

	t.Run("insert-multi-struct", func(t *testing.T) {
		session := engine.NewSession()
		defer func() {
			_ = session.Close()
		}()
		g := make([]*GenTable, 0)
		for b := 4; b <= 6; b++ {
			for c := 4; c <= 6; c++ {
				g = append(g, &GenTable{
					B: uint64(b),
					C: uint64(c),
				})
			}
		}

		_, err = session.Insert(&g)
		assert.NoError(t, err)

		for _, v := range g {
			log.Printf("[DEBUG]: %+v\n", *v)
			autoIncrID += 1
			assert.EqualValues(t, autoIncrID, (*v).A)
		}
	})
}
