package ydb

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type NullStruct struct {
	Id           int64 `xorm:"pk"`
	Name         sql.NullString
	Age          sql.NullInt64
	Height       sql.NullFloat64
	IsMan        sql.NullBool `xorm:"null"`
	Nil          driver.Valuer
	CustomStruct CustomStruct `xorm:"VARCHAR null"`
}

type CustomStruct struct {
	Year  int64
	Month int64
	Day   int64
}

func (CustomStruct) String() string {
	return "CustomStruct"
}

func (m *CustomStruct) Scan(value interface{}) error {
	if value == nil {
		m.Year, m.Month, m.Day = 0, 0, 0
		return nil
	}

	var s string
	switch t := value.(type) {
	case string:
		s = t
	case []byte:
		s = string(t)
	}
	if len(s) > 0 {
		seps := strings.Split(s, "/")
		Y, _ := strconv.Atoi(seps[0])
		M, _ := strconv.Atoi(seps[1])
		D, _ := strconv.Atoi(seps[2])
		m.Year = int64(Y)
		m.Month = int64(M)
		m.Day = int64(D)
		return nil
	}

	return fmt.Errorf("scan data %#v not fit []byte", value)
}

func (m CustomStruct) Value() (driver.Value, error) {
	return fmt.Sprintf("%d/%d/%d", m.Year, m.Month, m.Day), nil
}

func TestCreateNullStructTable(t *testing.T) {
	engine, err := enginePool.GetSchemeQueryEngine()
	assert.NoError(t, err)
	assert.NoError(t, engine.NewSession().DropTable(&NullStruct{}))
	assert.NoError(t, engine.NewSession().CreateTable(&NullStruct{}))
}

func TestDropNullStructTable(t *testing.T) {
	engine, err := enginePool.GetSchemeQueryEngine()
	assert.NoError(t, err)
	assert.NoError(t, engine.NewSession().DropTable(&NullStruct{}))
}

func TestNullStructInsert(t *testing.T) {
	assert.NoError(t, PrepareScheme(&NullStruct{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	items := []NullStruct{}
	for i := 0; i < 5; i++ {
		item := NullStruct{
			Id:           int64(i),
			Name:         sql.NullString{String: "haolei_" + fmt.Sprint(i+1), Valid: true},
			Age:          sql.NullInt64{Int64: 30 + int64(i), Valid: true},
			Height:       sql.NullFloat64{Float64: 1.5 + 1.1*float64(i), Valid: true},
			IsMan:        sql.NullBool{Bool: true, Valid: true},
			CustomStruct: CustomStruct{int64(i), int64(i + 1), int64(i + 2)},
			Nil:          nil,
		}
		items = append(items, item)
	}

	_, err = engine.Insert(&items)
	assert.NoError(t, err)

	items = make([]NullStruct, 0)
	err = engine.Find(&items)
	assert.NoError(t, err)
	assert.EqualValues(t, 5, len(items))
}

// FIXME
func TestNullStructUpdate(t *testing.T) {
	assert.NoError(t, PrepareScheme(&NullStruct{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	_, err = engine.InsertOne(NullStruct{
		Id: int64(1),
		Name: sql.NullString{
			String: "name1",
			Valid:  false,
		},
	})
	assert.NoError(t, err)

	_, err = engine.Insert([]NullStruct{
		{
			Id: int64(2),
			Name: sql.NullString{
				String: "name2",
				Valid:  true,
			},
		},
		{
			Id: int64(3),
			Name: sql.NullString{
				String: "name3",
				Valid:  true,
			},
		},
		{
			Id: int64(4),
			Name: sql.NullString{
				String: "name4",
				Valid:  true,
			},
		},
	})
	assert.NoError(t, err)

	if true { // 测试可插入NULL
		item := new(NullStruct)
		item.Age = sql.NullInt64{Int64: 23, Valid: true}
		item.Height = sql.NullFloat64{Float64: 0, Valid: true} // update to NULL

		_, err := engine.ID(int64(2)).Cols("age", "height", "is_man").Update(item)
		assert.NoError(t, err)
	}

	if true { // 测试In update
		item := new(NullStruct)
		item.Age = sql.NullInt64{Int64: 23, Valid: true}
		_, err := engine.In("id", int64(3), int64(4)).Cols("age", "height", "is_man").Update(item)
		assert.NoError(t, err)
	}

	if true { // 测试where
		item := new(NullStruct)
		item.Name = sql.NullString{String: "nullname", Valid: true}
		item.IsMan = sql.NullBool{Bool: true, Valid: true}
		item.Age = sql.NullInt64{Int64: 34, Valid: true}

		_, err := engine.Where("`age` > ?", int64(34)).Update(item)
		assert.NoError(t, err)
	}

	if true { // 修改全部时，插入空值
		// !datbeohbbh! YDB: if session.statement.ColumnStr() == ""
		// the 'arg' is inferred as <nil>, so can not correctly generate 'DECLARE' section
		t.Skipf("FIXME")
		item := &NullStruct{
			Name:   sql.NullString{String: "winxxp", Valid: true},
			Age:    sql.NullInt64{Int64: 30, Valid: true},
			Height: sql.NullFloat64{Float64: 1.72, Valid: true},
		}

		log.Println("BEGIN")
		_, err := engine.AllCols().Omit("id").ID(int64(6)).Update(item)
		log.Println("END")
		assert.NoError(t, err)
	}
}

func TestNullStructFind(t *testing.T) {
	assert.NoError(t, PrepareScheme(&NullStruct{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	_, err = engine.InsertOne(NullStruct{
		Id: int64(1),
		Name: sql.NullString{
			String: "name1",
			Valid:  false,
		},
	})
	assert.NoError(t, err)

	_, err = engine.Insert([]NullStruct{
		{
			Id: int64(2),
			Name: sql.NullString{
				String: "name2",
				Valid:  true,
			},
		},
		{
			Id: int64(3),
			Name: sql.NullString{
				String: "name3",
				Valid:  true,
			},
		},
		{
			Id: int64(4),
			Name: sql.NullString{
				String: "name4",
				Valid:  true,
			},
		},
	})
	assert.NoError(t, err)

	if true {
		item := new(NullStruct)
		has, err := engine.ID(int64(1)).Get(item)
		assert.NoError(t, err)
		assert.True(t, has)
		assert.EqualValues(t, 1, item.Id)
		assert.False(t, item.Name.Valid)
		assert.False(t, item.Age.Valid)
		assert.False(t, item.Height.Valid)
		assert.False(t, item.IsMan.Valid)
	}

	if true {
		item := new(NullStruct)
		item.Id = int64(2)
		has, err := engine.Get(item)
		assert.NoError(t, err)
		assert.True(t, has)
	}

	if true {
		item := make([]NullStruct, 0)
		err := engine.ID(int64(2)).Find(&item)
		assert.NoError(t, err)
	}

	if true {
		item := make([]NullStruct, 0)
		err := engine.Asc("age").Find(&item)
		assert.NoError(t, err)
	}
}

func TestNullStructIterate(t *testing.T) {
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	if true {
		err := engine.Where("`age` IS NOT NULL").OrderBy("age").Iterate(new(NullStruct),
			func(i int, bean interface{}) error {
				nultype := bean.(*NullStruct)
				fmt.Println(i, nultype)
				return nil
			})
		assert.NoError(t, err)
	}
}

func TestNullStructCount(t *testing.T) {
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	if true {
		item := new(NullStruct)
		_, err := engine.Where("`age` IS NOT NULL").Count(item)
		assert.NoError(t, err)
	}
}

func TestNullStructRows(t *testing.T) {
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	item := new(NullStruct)
	rows, err := engine.Where("`id` > ?", int64(1)).Rows(item)
	assert.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(item)
		assert.NoError(t, err)
	}
}

func TestNullStructDelete(t *testing.T) {
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	item := new(NullStruct)

	_, err = engine.ID(int64(1)).Delete(item)
	assert.NoError(t, err)

	_, err = engine.Where("`id` > ?", int64(1)).Delete(item)
	assert.NoError(t, err)
}
