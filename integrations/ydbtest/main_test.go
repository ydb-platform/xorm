// !datbeohbbh! This is CI test for YDB
// YDB has some features that are different from other RDBMS.
// So I decided to write its own tests.
// Note: Some tests in the original tests are copied.
package ydb

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	os.Exit(MainTest(m))
}
