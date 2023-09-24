package ydb

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTime(t *testing.T) {
	type TestTime struct {
		Uuid     string `xorm:"pk"`
		OperTime sql.NullTime
	}
	assert.NoError(t, PrepareScheme(&TestTime{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	tm := TestTime{
		Uuid:     "datbeohbb",
		OperTime: sql.NullTime{Time: time.Now().In(engine.GetTZLocation()), Valid: true},
	}

	_, err = engine.Insert(&tm)
	assert.NoError(t, err)

	var ret TestTime
	has, err := engine.Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.EqualValues(t, tm.OperTime.Time.Unix(), ret.OperTime.Time.Unix())
	assert.EqualValues(t, tm.OperTime.Time.Format(time.RFC3339), ret.OperTime.Time.Format(time.RFC3339))
}

func TestTimeInDiffLoc(t *testing.T) {
	type TestTime struct {
		Uuid     string `xorm:"pk"`
		OperTime *sql.NullTime
	}
	assert.NoError(t, PrepareScheme(&TestTime{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	newTzLoc, err := time.LoadLocation("Europe/Berlin")
	assert.NoError(t, err)

	newDbLoc, err := time.LoadLocation("America/New_York")
	assert.NoError(t, err)

	oldTzLoc := engine.GetTZLocation()
	oldDbLoc := engine.GetTZDatabase()

	defer func() {
		engine.SetTZLocation(oldTzLoc)
		engine.SetTZDatabase(oldDbLoc)
	}()

	engine.SetTZLocation(newTzLoc)
	engine.SetTZDatabase(newDbLoc)

	now := time.Now().In(newTzLoc)
	tm := TestTime{
		Uuid:     "datbeohbbh",
		OperTime: &sql.NullTime{Time: now, Valid: true},
	}

	_, err = engine.Insert(&tm)
	assert.NoError(t, err)

	var ret TestTime
	has, err := engine.Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)

	assert.EqualValues(t, tm.OperTime.Time.Unix(), ret.OperTime.Time.Unix())
	assert.EqualValues(t, tm.OperTime.Time.Format(time.RFC3339), ret.OperTime.Time.Format(time.RFC3339))
}

func TestTimeUserCreated(t *testing.T) {
	type TestTime struct {
		Uuid      string `xorm:"pk"`
		CreatedAt sql.NullTime
	}
	assert.NoError(t, PrepareScheme(&TestTime{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	tm := TestTime{
		Uuid: "datbeohbbh",
	}

	_, err = engine.Insert(&tm)
	assert.NoError(t, err)

	var ret TestTime
	has, err := engine.Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)

	t.Log(":", tm.CreatedAt)
	t.Log(":", ret.CreatedAt)

	assert.EqualValues(t, tm.CreatedAt.Time.UnixMicro(), ret.CreatedAt.Time.UnixMicro())
	assert.EqualValues(t, tm.CreatedAt.Time.Format(time.RFC3339), ret.CreatedAt.Time.Format(time.RFC3339))
}

func TestTimeUserCreatedDiffLoc(t *testing.T) {
	type TestTime struct {
		Uuid      string `xorm:"pk"`
		CreatedAt sql.NullTime
	}
	assert.NoError(t, PrepareScheme(&TestTime{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	newTzLoc, err := time.LoadLocation("Asia/Ho_Chi_Minh")
	assert.NoError(t, err)

	newDbLoc, err := time.LoadLocation("Europe/Berlin")
	assert.NoError(t, err)

	oldTzLoc := engine.GetTZLocation()
	oldDbLoc := engine.GetTZDatabase()

	defer func() {
		engine.SetTZLocation(oldTzLoc)
		engine.SetTZDatabase(oldDbLoc)
	}()

	engine.SetTZLocation(newTzLoc)
	engine.SetTZDatabase(newDbLoc)

	tm := TestTime{
		Uuid: "datbeohbbh",
	}

	_, err = engine.Insert(&tm)
	assert.NoError(t, err)

	ret := TestTime{}
	has, err := engine.Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)

	t.Log(":", tm.CreatedAt)
	t.Log(":", ret.CreatedAt)

	assert.EqualValues(t, tm.CreatedAt.Time.UnixMicro(), ret.CreatedAt.Time.UnixMicro())
	assert.EqualValues(t, tm.CreatedAt.Time.Format(time.RFC3339), ret.CreatedAt.Time.Format(time.RFC3339))
}

func TestTimeUserUpdated(t *testing.T) {
	type TestTime struct {
		Uuid      string `xorm:"pk"`
		Count     int64
		CreatedAt sql.NullTime
		UpdatedAt sql.NullTime
	}
	assert.NoError(t, PrepareScheme(&TestTime{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	tm := TestTime{
		Uuid: "datbeohbbh",
	}

	_, err = engine.Insert(&tm)
	assert.NoError(t, err)

	var ret TestTime
	has, err := engine.Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)

	t.Log("created 1:", tm.CreatedAt)
	t.Log("updated 1:", tm.UpdatedAt)
	t.Log("created 2:", ret.CreatedAt)
	t.Log("updated 2:", ret.UpdatedAt)

	assert.EqualValues(t, tm.CreatedAt.Time.UnixMicro(), ret.CreatedAt.Time.UnixMicro())
	assert.EqualValues(t, tm.UpdatedAt.Time.UnixMicro(), ret.UpdatedAt.Time.UnixMicro())
	assert.EqualValues(t, tm.CreatedAt.Time.Format(time.RFC3339), ret.CreatedAt.Time.Format(time.RFC3339))
	assert.EqualValues(t, tm.UpdatedAt.Time.Format(time.RFC3339), ret.UpdatedAt.Time.Format(time.RFC3339))

	tm2 := TestTime{
		CreatedAt: tm.CreatedAt,
	}
	_, err = engine.Incr("count", int64(1)).Update(&tm2, map[string]interface{}{"uuid": "datbeohbbh"})
	assert.NoError(t, err)

	ret = TestTime{}
	has, err = engine.Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)

	assert.EqualValues(t, tm2.CreatedAt.Time.UnixMicro(), ret.CreatedAt.Time.UnixMicro())
	assert.EqualValues(t, tm2.UpdatedAt.Time.UnixMicro(), ret.UpdatedAt.Time.UnixMicro())
	assert.EqualValues(t, tm2.CreatedAt.Time.Format(time.RFC3339), ret.CreatedAt.Time.Format(time.RFC3339))
	assert.EqualValues(t, tm2.UpdatedAt.Time.Format(time.RFC3339), ret.UpdatedAt.Time.Format(time.RFC3339))
}

func TestTimeUserUpdatedDiffLoc(t *testing.T) {
	type TestTime struct {
		Uuid      string `xorm:"pk"`
		Count     int64
		CreatedAt sql.NullTime
		UpdatedAt sql.NullTime
	}
	assert.NoError(t, PrepareScheme(&TestTime{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	newTzLoc, err := time.LoadLocation("Europe/Moscow")
	assert.NoError(t, err)

	newDbLoc, err := time.LoadLocation("Europe/Berlin")
	assert.NoError(t, err)

	oldTzLoc := engine.GetTZLocation()
	oldDbLoc := engine.GetTZDatabase()

	defer func() {
		engine.SetTZLocation(oldTzLoc)
		engine.SetTZDatabase(oldDbLoc)
	}()

	engine.SetTZLocation(newTzLoc)
	engine.SetTZDatabase(newDbLoc)

	tm := TestTime{
		Uuid: "datbeohbbh",
	}

	_, err = engine.Insert(&tm)
	assert.NoError(t, err)

	var ret TestTime
	has, err := engine.Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)

	t.Log("created 1:", tm.CreatedAt)
	t.Log("updated 1:", tm.UpdatedAt)
	t.Log("created 2:", ret.CreatedAt)
	t.Log("updated 2:", ret.UpdatedAt)

	assert.EqualValues(t, tm.CreatedAt.Time.UnixMicro(), ret.CreatedAt.Time.UnixMicro())
	assert.EqualValues(t, tm.UpdatedAt.Time.UnixMicro(), ret.UpdatedAt.Time.UnixMicro())
	assert.EqualValues(t, tm.CreatedAt.Time.Format(time.RFC3339), ret.CreatedAt.Time.Format(time.RFC3339))
	assert.EqualValues(t, tm.UpdatedAt.Time.Format(time.RFC3339), ret.UpdatedAt.Time.Format(time.RFC3339))

	tm2 := TestTime{
		CreatedAt: tm.CreatedAt,
	}
	_, err = engine.Incr("count", int64(1)).Update(&tm2, map[string]interface{}{"uuid": "datbeohbbh"})
	assert.NoError(t, err)

	ret = TestTime{}
	has, err = engine.Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)

	assert.EqualValues(t, tm2.CreatedAt.Time.UnixMicro(), ret.CreatedAt.Time.UnixMicro())
	assert.EqualValues(t, tm2.UpdatedAt.Time.UnixMicro(), ret.UpdatedAt.Time.UnixMicro())
	assert.EqualValues(t, tm2.CreatedAt.Time.Format(time.RFC3339), ret.CreatedAt.Time.Format(time.RFC3339))
	assert.EqualValues(t, tm2.UpdatedAt.Time.Format(time.RFC3339), ret.UpdatedAt.Time.Format(time.RFC3339))
}

func TestFindTimeDiffLoc(t *testing.T) {
	type TestTime struct {
		Uuid     string       `xorm:"pk 'uuid'"`
		OperTime sql.NullTime `xorm:"'oper_time'"`
	}
	assert.NoError(t, PrepareScheme(&TestTime{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	newTzLoc, err := time.LoadLocation("America/New_York")
	assert.NoError(t, err)

	newDbLoc, err := time.LoadLocation("Europe/Berlin")
	assert.NoError(t, err)

	oldTzLoc := engine.GetTZLocation()
	oldDbLoc := engine.GetTZDatabase()

	defer func() {
		engine.SetTZLocation(oldTzLoc)
		engine.SetTZDatabase(oldDbLoc)
	}()

	engine.SetTZLocation(newTzLoc)
	engine.SetTZDatabase(newDbLoc)

	session := engine.NewSession()
	defer session.Close()

	var (
		now      = time.Now().In(newTzLoc)
		expected = make([]TestTime, 0)
		actual   = make([]TestTime, 0)
	)

	for i := 0; i < 10; i++ {
		now = now.Add(time.Minute).In(newTzLoc)
		data := TestTime{
			Uuid:     fmt.Sprintf("%d", i),
			OperTime: sql.NullTime{Time: now, Valid: true},
		}
		_, err = session.Insert(&data)
		assert.NoError(t, err)
		expected = append(expected, data)
	}

	err = session.Table(&TestTime{}).Asc("oper_time").Find(&actual)
	assert.NoError(t, err)
	assert.EqualValues(t, len(expected), len(actual))

	for i, e := range expected {
		assert.EqualValues(t, e.OperTime.Time.Unix(), actual[i].OperTime.Time.Unix())
		assert.EqualValues(t, e.OperTime.Time.Format(time.RFC3339), actual[i].OperTime.Time.Format(time.RFC3339))
	}

	t.Log(expected)
	t.Log(actual)
}
