package ydb

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"xorm.io/builder"
)

func TestFindJoinLimit(t *testing.T) {
	type Salary struct {
		Uuid int64 `xorm:"pk"`
		Lid  int64
	}

	type CheckList struct {
		Uuid int64 `xorm:"pk"`
		Eid  int64
	}

	type Empsetting struct {
		Uuid int64 `xorm:"pk"`
		Name string
	}
	assert.NoError(t, PrepareScheme(&Salary{}, &CheckList{}, &Empsetting{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	emp := Empsetting{
		Uuid: int64(1),
		Name: "datbeohbbh",
	}

	session := engine.NewSession()
	defer session.Close()

	_, err = session.Insert(&emp)
	assert.NoError(t, err)

	checklist := CheckList{Uuid: int64(1), Eid: emp.Uuid}
	_, err = session.Insert(&checklist)
	assert.NoError(t, err)

	salary := Salary{Uuid: int64(1), Lid: checklist.Uuid}
	_, err = session.Insert(&salary)
	assert.NoError(t, err)

	var salaries []Salary
	err = engine.Table("salary").
		Cols("`salary`.`uuid`", "`salary`.`lid`").
		Join("INNER", "check_list", "`check_list`.`uuid` = `salary`.`lid`").
		Join("LEFT", "empsetting", "`empsetting`.`uuid` = `check_list`.`eid`").
		Limit(10, 0).
		Find(&salaries)
	assert.NoError(t, err)
}

func TestFindCond(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()

	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	users := make([]Users, 0)

	err = engine.Where("user_id > ?", sql.NullInt64{Int64: 5, Valid: true}).Find(&users)
	assert.NoError(t, err)
	assert.Equal(t, len(usersData)-6, len(users))

	users = []Users{}
	err = engine.
		Where(builder.Between{
			Col:     "user_id",
			LessVal: sql.NullInt64{Int64: 5, Valid: true},
			MoreVal: sql.NullInt64{Int64: 15, Valid: true},
		}).
		Where(builder.Gt{"age": int32(30)}).
		Find(&users)
	assert.NoError(t, err)
	assert.Equal(t, 7, len(users))
}

func TestFindMap(t *testing.T) {
	type Salary struct {
		Uuid int64 `xorm:"pk"`
		Lid  int64
	}

	assert.NoError(t, PrepareScheme(&Salary{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	salaryData := []Salary{}
	for uuid := 0; uuid <= 20; uuid++ {
		salaryData = append(salaryData, Salary{
			Uuid: int64(uuid),
		})
	}

	_, err = engine.Insert(&salaryData)
	assert.NoError(t, err)

	salaries := make(map[int64]Salary)

	err = engine.Find(&salaries)
	assert.NoError(t, err)
	assert.Equal(t, len(salaryData), len(salaries))

	salariesPtr := map[int64]*Salary{}
	err = engine.Cols("lid").Find(&salariesPtr)
	assert.NoError(t, err)
	assert.Equal(t, len(salaryData), len(salariesPtr))
}

func TestFindDistinct(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()
	for w, g := len(usersData)/4, 0; g < 4; g++ {
		for i := 0; i < w; i++ {
			usersData[w*g+i].Age = uint32(22 + g)
		}
	}

	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	users := []Users{}
	err = engine.Distinct("age").Find(&users)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(users))
}

func TestFindOrder(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()
	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	users := make([]Users, 0)
	err = engine.OrderBy("`user_id` desc").Find(&users)
	assert.NoError(t, err)
	assert.Equal(t, len(usersData), len(users))

	for i := len(usersData) - 1; i >= 0; i-- {
		assert.Equal(t, usersData[i].UserID, users[len(users)-i-1].UserID)
	}

	users = []Users{}
	err = engine.Asc("user_id").Find(&users)
	assert.NoError(t, err)
	assert.Equal(t, len(usersData), len(users))
	for i := 0; i < len(usersData); i++ {
		assert.Equal(t, usersData[i].UserID, users[i].UserID)
	}
}

func TestFindGroupBy(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()
	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	users := make([]Users, 0)
	err = engine.GroupBy("`age`, `user_id`, `number`").Find(&users)
	assert.NoError(t, err)
	assert.Equal(t, len(usersData), len(users))
}

func TestFindHaving(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()
	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	users := make([]Users, 0)
	err = engine.
		GroupBy("`age`, `user_id`, `number`").
		Having("`user_id` = 0").
		Find(&users)
	assert.NoError(t, err)
}

func TestFindInt(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()
	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	age := []uint32{}
	err = engine.Table(&Users{}).Cols("age").Asc().Find(&age)
	assert.NoError(t, err)
	assert.Equal(t, len(usersData), len(age))

	userIds := []int64{}
	err = engine.Table(&Users{}).Cols("user_id").Desc("user_id").Find(&userIds)
	assert.NoError(t, err)
	assert.Equal(t, len(usersData), len(userIds))
}

func TestFindString(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()
	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	expectedName := []string{}
	for _, user := range usersData {
		expectedName = append(expectedName, user.Name)
	}

	expectedNumber := []string{}
	for _, user := range usersData {
		expectedNumber = append(expectedNumber, user.Number)
	}

	names := []string{}
	err = engine.Table(&Users{}).Cols("name").Asc("name").Find(&names)
	assert.NoError(t, err)
	assert.Equal(t, len(usersData), len(names))
	assert.ElementsMatch(t, expectedName, names)

	numbers := []string{}
	err = engine.Table(&Users{}).Cols("number").Find(&numbers)
	assert.NoError(t, err)
	assert.Equal(t, len(usersData), len(numbers))
	assert.ElementsMatch(t, expectedNumber, numbers)
}

func TestFindCustomType(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()
	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	type cstring string

	expectedName := []cstring{}
	for _, user := range usersData {
		expectedName = append(expectedName, cstring(user.Name))
	}

	names := []cstring{}
	err = engine.Table(&Users{}).Cols("name").Asc("name").Find(&names)
	assert.NoError(t, err)
	assert.Equal(t, len(usersData), len(names))
	assert.ElementsMatch(t, expectedName, names)
}

func TestFindInterface(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()
	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	age := []interface{}{}
	err = engine.Table(&Users{}).Cols("age").Asc("age").Find(&age)
	assert.NoError(t, err)
	assert.Equal(t, len(usersData), len(age))

	for i := 0; i < len(usersData); i++ {
		assert.Equal(t, usersData[i].Age, age[i].(uint32))
	}
}

func TestFindSliceBytes(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Series{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	seriesData, _, _ := getData()
	_, err = engine.Insert(&seriesData)
	assert.NoError(t, err)

	expectedSeriesId := []string{}
	for _, series := range seriesData {
		expectedSeriesId = append(expectedSeriesId, string(series.SeriesID))
	}

	seriesIds := make([]string, 0)
	err = engine.Table(&Series{}).Cols("series_id").Find(&seriesIds)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expectedSeriesId, seriesIds)
}

func TestFindBool(t *testing.T) {
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	type FindBoolStruct struct {
		Uuid int64 `xorm:"pk"`
		Msg  bool
	}

	assert.NoError(t, PrepareScheme(&FindBoolStruct{}))

	_, err = engine.Insert([]FindBoolStruct{
		{
			Uuid: int64(1),
			Msg:  false,
		},
		{
			Uuid: int64(2),
			Msg:  true,
		},
	})
	assert.NoError(t, err)

	results := make([]FindBoolStruct, 0)
	err = engine.Asc("uuid").Find(&results)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, len(results))

	assert.False(t, results[0].Msg)
	assert.True(t, results[1].Msg)
}

func TestFindAndCount(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()
	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	users := make([]Users, 0)
	cnt, err := engine.FindAndCount(&users)
	assert.NoError(t, err)
	assert.EqualValues(t, len(usersData), cnt)

	users = []Users{}
	_, err = engine.Limit(10, 0).FindAndCount(&users)
	assert.NoError(t, err)
	assert.EqualValues(t, 10, len(users))
}

func TestFindAndCountDistinct(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()
	for w, g := len(usersData)/4, 0; g < 4; g++ {
		for i := 0; i < w; i++ {
			usersData[w*g+i].Age = uint32(22 + g)
		}
	}

	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	users := make([]Users, 0)
	cnt, err := engine.Distinct("age").FindAndCount(&users)
	assert.NoError(t, err)
	assert.EqualValues(t, 4, cnt)
	assert.EqualValues(t, 4, len(users))
}

func TestFindAndCountGroupBy(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()
	for w, g := len(usersData)/4, 0; g < 4; g++ {
		for i := 0; i < w; i++ {
			usersData[w*g+i].Age = uint32(22 + g)
		}
	}

	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	users := make([]Users, 0)
	cnt, err := engine.GroupBy("age").FindAndCount(&users)
	assert.NoError(t, err)
	assert.EqualValues(t, 4, cnt)
	assert.EqualValues(t, 4, len(users))
}

func TestFindTime(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	usersData := getUsersData()
	_, err = engine.Insert(&usersData)
	assert.NoError(t, err)

	createdAt := make([]string, 0)
	err = engine.Table(&Users{}).Cols("created_at").Find(&createdAt)
	assert.NoError(t, err)
	assert.EqualValues(t, len(usersData), len(createdAt))
}

func TestFindStringArray(t *testing.T) {
	type TestString struct {
		Id   string    `xorm:"pk VARCHAR"`
		Data *[]string `xorm:"TEXT"`
	}

	assert.NoError(t, PrepareScheme(&TestString{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	_, err = engine.Insert(&TestString{
		Id:   uuid.NewString(),
		Data: &[]string{"a", "b", "c"},
	})
	assert.NoError(t, err)

	var ret TestString
	has, err := engine.Get(&ret)
	assert.NoError(t, err)
	assert.True(t, has)

	assert.EqualValues(t, []string{"a", "b", "c"}, *(ret.Data))

	for i := 0; i < 10; i++ {
		_, err = engine.Insert(&TestString{
			Id:   uuid.NewString(),
			Data: &[]string{"a", "b", "c"},
		})
		assert.NoError(t, err)
	}

	var arr []TestString
	err = engine.Asc("id").Find(&arr)
	assert.NoError(t, err)

	for _, v := range arr {
		res := *(v.Data)
		assert.EqualValues(t, []string{"a", "b", "c"}, res)
	}
}

func TestFindCustomTypeAllField(t *testing.T) {
	type RowID = uint64
	type Str = *string
	type Double = *float64
	type Timestamp = *time.Time

	type Row struct {
		ID               RowID     `xorm:"pk 'id'"`
		PayloadStr       Str       `xorm:"'payload_str'"`
		PayloadDouble    Double    `xorm:"'payload_double'"`
		PayloadTimestamp Timestamp `xorm:"'payload_timestamp'"`
	}

	rows := make([]Row, 0)
	for i := 0; i < 10; i++ {
		rows = append(rows, Row{
			ID:               RowID(i),
			PayloadStr:       func(s string) *string { return &s }(fmt.Sprintf("payload#%d", i)),
			PayloadDouble:    func(f float64) *float64 { return &f }((float64)(i)),
			PayloadTimestamp: func(t time.Time) *time.Time { return &t }(time.Now()),
		})
	}

	assert.NoError(t, PrepareScheme(&Row{}))
	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)

	session := engine.NewSession()
	defer session.Close()

	_, err = session.Insert(&rows)
	assert.NoError(t, err)

	cnt, err := session.Count(&Row{})
	assert.NoError(t, err)
	assert.EqualValues(t, 10, cnt)

	res := make([]Row, 0)
	err = session.Asc("id").Find(&res)
	assert.NoError(t, err)
	assert.EqualValues(t, len(rows), len(res))

	for i, v := range rows {
		assert.EqualValues(t, v.ID, res[i].ID)
		assert.EqualValues(t, v.PayloadStr, res[i].PayloadStr)
		assert.EqualValues(t, v.PayloadDouble, res[i].PayloadDouble)
		assert.EqualValues(t, v.PayloadTimestamp.Unix(), res[i].PayloadTimestamp.Unix())
	}
}

func TestFindSqlNullable(t *testing.T) {
	type SqlNullable struct {
		ID     sql.NullInt64  `xorm:"pk 'id'"`
		Bool   *sql.NullBool  `xorm:"'bool'"`
		Int32  *sql.NullInt32 `xorm:"'int32'"`
		String sql.NullString `xorm:"'string'"`
		Time   *sql.NullTime  `xorm:"'time'"`
	}

	assert.NoError(t, PrepareScheme(&SqlNullable{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	oldTzLoc := engine.GetTZLocation()
	oldDbLoc := engine.GetTZDatabase()

	defer func() {
		engine.SetTZLocation(oldTzLoc)
		engine.SetTZDatabase(oldDbLoc)
	}()

	engine.SetTZLocation(time.UTC)
	engine.SetTZDatabase(time.UTC)

	data := make([]*SqlNullable, 0)
	for i := 0; i < 10; i++ {
		data = append(data, &SqlNullable{
			ID:     sql.NullInt64{Int64: int64(i), Valid: true},
			Bool:   &sql.NullBool{},
			Int32:  &sql.NullInt32{Int32: int32(i), Valid: true},
			String: sql.NullString{String: fmt.Sprintf("data#%d", i), Valid: true},
			Time:   &sql.NullTime{Time: time.Now().In(time.UTC), Valid: true},
		})
	}

	session := engine.NewSession()
	defer session.Close()

	_, err = session.Insert(&data)
	assert.NoError(t, err)

	res := make([]*SqlNullable, 0)
	err = session.Table(&SqlNullable{}).OrderBy("id").Find(&res)
	assert.NoError(t, err)

	for i, v := range data {
		assert.EqualValues(t, v.ID, res[i].ID)
		assert.Nil(t, res[i].Bool)
		assert.EqualValues(t, v.Int32, res[i].Int32)
		assert.EqualValues(t, v.String, res[i].String)
		assert.EqualValues(t, v.Time.Time.Format(time.RFC3339), res[i].Time.Time.Format(time.RFC3339))
	}
}

func TestFindEmptyField(t *testing.T) {
	type EmptyField struct {
		ID uint64 `xorm:"pk 'id'"`

		Bool bool

		Int64  int64
		Uint64 uint64

		Int32  int32
		Uint32 uint32

		Uint8 uint8

		Float  float32
		Double float64

		Utf8 string

		Timestamp time.Time

		Interval time.Duration

		String []byte
	}

	PrepareScheme(&EmptyField{})

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	data := make([]EmptyField, 0)
	for i := 0; i < 10; i++ {
		data = append(data, EmptyField{
			ID: uint64(i),
		})
		data[i].String = []uint8{}
	}

	_, err = engine.Insert(&data)
	assert.NoError(t, err)

	res := make([]EmptyField, 0)
	err = engine.Asc("id").Find(&res)
	assert.NoError(t, err)

	assert.Equal(t, data, res)
}
