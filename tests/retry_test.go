package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"xorm.io/xorm"
	"xorm.io/xorm/retry"
)

func TestRetry(t *testing.T) {
	assert.NoError(t, PrepareEngine())

	type TestRetry struct {
		Id   int64  `xorm:"int(11) pk"`
		Name string `xorm:"varchar(255)"`
	}

	assert.NoError(t, testEngine.Sync(new(TestRetry)))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := testEngine.Do(ctx, func(ctx context.Context, session *xorm.Session) error {
		num, err := insertMultiDatas(1,
			append([]TestRetry{}, TestRetry{1, "test1"}, TestRetry{2, "test2"}, TestRetry{3, "test3"}))

		if err != nil {
			return err
		}

		assert.EqualValues(t, 3, num)
		return nil
	}, retry.WithID("test-retry"))

	assert.NoError(t, err)
}

func TestRetryTx(t *testing.T) {
	assert.NoError(t, PrepareEngine())
	assertSync(t, new(Userinfo))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := testEngine.DoTx(
		ctx,
		func(ctx context.Context, session *xorm.Session) error {
			user1 := Userinfo{Username: "xiaoxiao2", Departname: "dev", Alias: "lunny", Created: time.Now()}
			if _, err := session.Insert(&user1); err != nil {
				return err
			}

			user2 := Userinfo{Username: "zzz"}
			if _, err := session.Where("`id` = ?", 0).Update(&user2); err != nil {
				return err
			}

			if _, err := session.Exec("delete from "+testEngine.Quote(testEngine.TableName("userinfo", true))+" where `username` = ?", user2.Username); err != nil {
				return err
			}

			return nil
		},
		retry.WithID("test-retry-tx"),
		retry.WithMaxRetries(5),
	)

	assert.NoError(t, err)
}
