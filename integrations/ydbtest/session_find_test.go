package ydb

import (
	"database/sql"
	"testing"

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
