package ydb

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"xorm.io/builder"
)

func TestTime(t *testing.T) {
	type TestTime struct {
		Uuid     string `xorm:"pk"`
		OperTime time.Time
	}
	assert.NoError(t, PrepareScheme(&TestTime{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	tm := TestTime{
		Uuid:     "datbeohbb",
		OperTime: time.Now().In(engine.GetTZLocation()),
	}

	_, err = engine.Insert(&tm)
	assert.NoError(t, err)

	var ret TestTime
	has, err := engine.Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.EqualValues(t, tm.OperTime.Unix(), ret.OperTime.Unix())
	assert.EqualValues(t, tm.OperTime.Format(time.RFC3339), ret.OperTime.Format(time.RFC3339))
}

func TestTimeInDiffLoc(t *testing.T) {
	type TestTime struct {
		Uuid     string `xorm:"pk"`
		OperTime *time.Time
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
		OperTime: &now,
	}

	_, err = engine.Insert(&tm)
	assert.NoError(t, err)

	var ret TestTime
	has, err := engine.Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)

	assert.EqualValues(t, tm.OperTime.Unix(), ret.OperTime.Unix())
	assert.EqualValues(t, tm.OperTime.Format(time.RFC3339), ret.OperTime.Format(time.RFC3339))
}

func TestTimeUserCreated(t *testing.T) {
	type TestTime struct {
		Uuid      string    `xorm:"pk"`
		CreatedAt time.Time `xorm:"created"`
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

	assert.EqualValues(t, tm.CreatedAt.UnixMicro(), ret.CreatedAt.UnixMicro())
	assert.EqualValues(t, tm.CreatedAt.Format(time.RFC3339), ret.CreatedAt.Format(time.RFC3339))
}

func TestTimeUserCreatedDiffLoc(t *testing.T) {
	type TestTime struct {
		Uuid      string    `xorm:"pk"`
		CreatedAt time.Time `xorm:"created"`
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

	assert.EqualValues(t, tm.CreatedAt.UnixMicro(), ret.CreatedAt.UnixMicro())
	assert.EqualValues(t, tm.CreatedAt.Format(time.RFC3339), ret.CreatedAt.Format(time.RFC3339))
}

func TestTimeUserUpdated(t *testing.T) {
	type TestTime struct {
		Uuid      string `xorm:"pk"`
		Count     int64
		CreatedAt time.Time `xorm:"created"`
		UpdatedAt time.Time `xorm:"updated"`
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

	assert.EqualValues(t, tm.CreatedAt.UnixMicro(), ret.CreatedAt.UnixMicro())
	assert.EqualValues(t, tm.UpdatedAt.UnixMicro(), ret.UpdatedAt.UnixMicro())
	assert.EqualValues(t, tm.CreatedAt.Format(time.RFC3339), ret.CreatedAt.Format(time.RFC3339))
	assert.EqualValues(t, tm.UpdatedAt.Format(time.RFC3339), ret.UpdatedAt.Format(time.RFC3339))

	tm2 := TestTime{
		CreatedAt: tm.CreatedAt,
	}
	_, err = engine.Incr("count", int64(1)).Update(&tm2, builder.Eq{"uuid": "datbeohbbh"})
	assert.NoError(t, err)

	ret = TestTime{}
	has, err = engine.Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)

	assert.EqualValues(t, tm2.CreatedAt.UnixMicro(), ret.CreatedAt.UnixMicro())
	assert.EqualValues(t, tm2.UpdatedAt.UnixMicro(), ret.UpdatedAt.UnixMicro())
	assert.EqualValues(t, tm2.CreatedAt.Format(time.RFC3339), ret.CreatedAt.Format(time.RFC3339))
	assert.EqualValues(t, tm2.UpdatedAt.Format(time.RFC3339), ret.UpdatedAt.Format(time.RFC3339))
}

func TestTimeUserUpdatedDiffLoc(t *testing.T) {
	type TestTime struct {
		Uuid      string `xorm:"pk"`
		Count     int64
		CreatedAt time.Time `xorm:"created"`
		UpdatedAt time.Time `xorm:"updated"`
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

	assert.EqualValues(t, tm.CreatedAt.UnixMicro(), ret.CreatedAt.UnixMicro())
	assert.EqualValues(t, tm.UpdatedAt.UnixMicro(), ret.UpdatedAt.UnixMicro())
	assert.EqualValues(t, tm.CreatedAt.Format(time.RFC3339), ret.CreatedAt.Format(time.RFC3339))
	assert.EqualValues(t, tm.UpdatedAt.Format(time.RFC3339), ret.UpdatedAt.Format(time.RFC3339))

	tm2 := TestTime{
		CreatedAt: tm.CreatedAt,
	}
	_, err = engine.Incr("count", int64(1)).Update(&tm2, builder.Eq{"uuid": "datbeohbbh"})
	assert.NoError(t, err)

	ret = TestTime{}
	has, err = engine.Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)

	assert.EqualValues(t, tm2.CreatedAt.UnixMicro(), ret.CreatedAt.UnixMicro())
	assert.EqualValues(t, tm2.UpdatedAt.UnixMicro(), ret.UpdatedAt.UnixMicro())
	assert.EqualValues(t, tm2.CreatedAt.Format(time.RFC3339), ret.CreatedAt.Format(time.RFC3339))
	assert.EqualValues(t, tm2.UpdatedAt.Format(time.RFC3339), ret.UpdatedAt.Format(time.RFC3339))
}

type JSONDate time.Time

func (j JSONDate) MarshalJSON() ([]byte, error) {
	if time.Time(j).IsZero() {
		return []byte(`""`), nil
	}
	return []byte(`"` + time.Time(j).Format("2006-01-02 15:04:05") + `"`), nil
}

func (j *JSONDate) UnmarshalJSON(value []byte) error {
	var v = strings.TrimSpace(strings.Trim(string(value), "\""))

	t, err := time.ParseInLocation("2006-01-02 15:04:05", v, time.Local)
	if err != nil {
		return err
	}
	*j = JSONDate(t)
	return nil
}

func (j *JSONDate) Unix() int64 {
	return (*time.Time)(j).Unix()
}

func TestCustomTimeUser(t *testing.T) {
	type TestTime struct {
		Id        string   `xorm:"pk"`
		CreatedAt JSONDate `xorm:"created"`
		UpdatedAt JSONDate `xorm:"updated"`
	}

	assert.NoError(t, PrepareScheme(&TestTime{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	var user = TestTime{
		Id: "datbeohbbh",
	}

	_, err = engine.Insert(&user)
	assert.NoError(t, err)
	t.Log("user", user.CreatedAt, user.UpdatedAt)

	var user2 TestTime
	has, err := engine.Get(&user2)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.EqualValues(t, user.CreatedAt.Unix(), user2.CreatedAt.Unix())
	assert.EqualValues(t, time.Time(user.CreatedAt).Format(time.RFC3339), time.Time(user2.CreatedAt).Format(time.RFC3339))
	assert.EqualValues(t, user.UpdatedAt.Unix(), user2.UpdatedAt.Unix())
	assert.EqualValues(t, time.Time(user.UpdatedAt).Format(time.RFC3339), time.Time(user2.UpdatedAt).Format(time.RFC3339))
}

func TestFindTimeDiffLoc(t *testing.T) {
	type TestTime struct {
		Uuid     string    `xorm:"pk 'uuid'"`
		OperTime time.Time `xorm:"'oper_time'"`
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
			OperTime: now,
		}
		_, err = session.Insert(&data)
		assert.NoError(t, err)
		expected = append(expected, data)
	}

	err = session.Table(&TestTime{}).Asc("oper_time").Find(&actual)
	assert.NoError(t, err)
	assert.EqualValues(t, len(expected), len(actual))

	for i, e := range expected {
		assert.EqualValues(t, e.OperTime.Unix(), actual[i].OperTime.Unix())
		assert.EqualValues(t, e.OperTime.Format(time.RFC3339), actual[i].OperTime.Format(time.RFC3339))
	}

	t.Log(expected)
	t.Log(actual)
}
