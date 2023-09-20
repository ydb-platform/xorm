// !datbeohbbh! This is CI test for YDB
// YDB has some features that are different from other RDBMS.
// Such as, no auto increment primary key, various query mode, transaction query structure.
// So I decided to write its own tests.
// Note: Some tests in the original tests are copied.
package ydb

import (
	"os"
	"testing"

	_ "github.com/ydb-platform/ydb-go-sdk/v3"
)

func TestMain(m *testing.M) {
	os.Exit(MainTest(m))
}
