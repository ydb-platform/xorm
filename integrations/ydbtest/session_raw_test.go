package ydb

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExecAndQuery(t *testing.T) {
	type UserinfoQuery struct {
		Uid  int64 `xorm:"pk"`
		Name string
	}

	assert.NoError(t, PrepareScheme(&UserinfoQuery{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	_, err = engine.
		Exec("INSERT INTO "+engine.TableName("`userinfo_query`", true)+" (`uid`, `name`) VALUES (?, ?)", int64(1), "user")
	assert.NoError(t, err)

	results, err := engine.
		Query("select * from " + engine.Quote(engine.TableName("userinfo_query", true)))
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(results))

	id, err := strconv.Atoi(string(results[0]["uid"]))
	assert.NoError(t, err)
	assert.EqualValues(t, 1, id)
	assert.Equal(t, "user", string(results[0]["name"]))
}

func TestExecTime(t *testing.T) {
	type UserinfoExecTime struct {
		Uid     int64 `xorm:"pk"`
		Name    string
		Created time.Time
	}

	assert.NoError(t, PrepareScheme(&UserinfoExecTime{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	now := time.Now()
	_, err = engine.
		Exec("INSERT INTO "+engine.TableName("`userinfo_exec_time`", true)+" (`uid`, `name`, `created`) VALUES (?, ?, ?)", int64(1), "user", now)
	assert.NoError(t, err)

	var uet UserinfoExecTime
	has, err := engine.Where("`uid`=?", int64(1)).Get(&uet)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.EqualValues(t, now.In(engine.GetTZLocation()).Format(time.RFC3339), uet.Created.Format(time.RFC3339))
}
