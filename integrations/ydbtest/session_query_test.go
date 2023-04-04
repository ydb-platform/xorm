package ydb

import (
	"strconv"
	"testing"
	"time"

	"xorm.io/builder"

	"github.com/stretchr/testify/assert"
)

func TestQueryString(t *testing.T) {
	type GetVar2 struct {
		Uuid    int64  `xorm:"pk"`
		Msg     string `xorm:"varchar(255)"`
		Age     int
		Money   float64
		Created time.Time `xorm:"created"`
	}

	assert.NoError(t, PrepareScheme(&GetVar2{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	var data = GetVar2{
		Uuid:  int64(1),
		Msg:   "hi",
		Age:   28,
		Money: 1.5,
	}
	_, err = engine.InsertOne(data)
	assert.NoError(t, err)

	records, err := engine.QueryString("select * from " + engine.Quote(engine.TableName("get_var2", true)))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(records))
	assert.Equal(t, 5, len(records[0]))
	assert.Equal(t, "1", records[0]["uuid"])
	assert.Equal(t, "hi", records[0]["msg"])
	assert.Equal(t, "28", records[0]["age"])
	assert.Equal(t, "1.5", records[0]["money"])
}

func TestQueryString2(t *testing.T) {
	type GetVar3 struct {
		Uuid int64 `xorm:"pk"`
		Msg  bool
	}

	assert.NoError(t, PrepareScheme(&GetVar3{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	var data = GetVar3{
		Uuid: int64(1),
		Msg:  false,
	}
	_, err = engine.Insert(data)
	assert.NoError(t, err)

	records, err := engine.QueryString("select * from " + engine.Quote(engine.TableName("get_var3", true)))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(records))
	assert.Equal(t, 2, len(records[0]))
	assert.Equal(t, "1", records[0]["uuid"])
	assert.True(t, "false" == records[0]["msg"])
}

func toBool(i interface{}) bool {
	switch t := i.(type) {
	case int32:
		return t > 0
	case bool:
		return t
	}
	return false
}

func TestQueryInterface(t *testing.T) {
	type GetVarInterface struct {
		Uuid    int64  `xorm:"pk"`
		Msg     string `xorm:"varchar(255)"`
		Age     int32
		Money   float64
		Created time.Time `xorm:"created"`
	}

	assert.NoError(t, PrepareScheme(&GetVarInterface{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	var data = GetVarInterface{
		Uuid:  int64(1),
		Msg:   "hi",
		Age:   int32(28),
		Money: 1.5,
	}
	_, err = engine.InsertOne(data)
	assert.NoError(t, err)

	records, err := engine.QueryInterface("select * from " + engine.Quote(engine.TableName("get_var_interface", true)))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(records))
	assert.Equal(t, 5, len(records[0]))
	/* 	assert.EqualValues(t, "1", string(records[0]["uuid"].([]byte)))
	   	assert.Equal(t, "hi", string(records[0]["msg"].([]byte)))
	   	assert.EqualValues(t, "28", string(records[0]["age"].([]byte)))
	   	assert.EqualValues(t, "1.5", string(records[0]["money"].([]byte))) */
	assert.EqualValues(t, 1, records[0]["uuid"].(int64))
	assert.Equal(t, "hi", string(records[0]["msg"].(string)))
	assert.EqualValues(t, 28, records[0]["age"].(int32))
	assert.EqualValues(t, 1.5, records[0]["money"].(float64))
}

func TestQueryNoParams(t *testing.T) {
	type QueryNoParams struct {
		Uuid    int64  `xorm:"pk"`
		Msg     string `xorm:"varchar(255)"`
		Age     int
		Money   float64
		Created time.Time `xorm:"created"`
	}

	assert.NoError(t, PrepareScheme(&QueryNoParams{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	engine.ShowSQL(true)

	var q = QueryNoParams{
		Uuid:  int64(1),
		Msg:   "message",
		Age:   20,
		Money: 3000,
	}
	_, err = engine.Insert(&q)
	assert.NoError(t, err)

	assertResult := func(t *testing.T, results []map[string][]byte) {
		assert.EqualValues(t, 1, len(results))
		id, err := strconv.ParseInt(string(results[0]["uuid"]), 10, 64)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, id)
		assert.Equal(t, "message", string(results[0]["msg"]))

		age, err := strconv.Atoi(string(results[0]["age"]))
		assert.NoError(t, err)
		assert.EqualValues(t, 20, age)

		money, err := strconv.ParseFloat(string(results[0]["money"]), 32)
		assert.NoError(t, err)
		assert.EqualValues(t, 3000, money)
	}

	results, err := engine.Table("query_no_params").Limit(10).Query()
	assert.NoError(t, err)
	assertResult(t, results)

	results, err = engine.SQL("select * from " + engine.Quote(engine.TableName("query_no_params", true))).Query()
	assert.NoError(t, err)
	assertResult(t, results)
}

func TestQueryStringNoParam(t *testing.T) {
	type GetVar4 struct {
		Uuid int64 `xorm:"pk"`
		Msg  bool
	}

	assert.NoError(t, PrepareScheme(&GetVar4{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	var data = GetVar4{
		Uuid: int64(1),
		Msg:  false,
	}
	_, err = engine.Insert(data)
	assert.NoError(t, err)

	records, err := engine.Table("get_var4").Limit(1).QueryString()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(records))
	assert.EqualValues(t, "1", records[0]["uuid"])
	assert.EqualValues(t, "false", records[0]["msg"])

	records, err = engine.Table("get_var4").Where(builder.Eq{"`uuid`": int64(1)}).QueryString()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(records))
	assert.EqualValues(t, "1", records[0]["uuid"])
	assert.EqualValues(t, "false", records[0]["msg"])
}

func TestQuerySliceStringNoParam(t *testing.T) {
	type GetVar6 struct {
		Uuid int64 `xorm:"pk"`
		Msg  bool
	}

	assert.NoError(t, PrepareScheme(&GetVar6{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	var data = GetVar6{
		Uuid: int64(1),
		Msg:  false,
	}
	_, err = engine.Insert(data)
	assert.NoError(t, err)

	records, err := engine.Table("get_var6").Cols("uuid", "msg").Limit(1).QuerySliceString()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(records))
	assert.EqualValues(t, "1", records[0][0])
	assert.EqualValues(t, "false", records[0][1])

	records, err = engine.
		Table("get_var6").
		Cols("uuid", "msg").
		Where(builder.Eq{"`uuid`": int64(1)}).
		QuerySliceString()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(records))
	assert.EqualValues(t, "1", records[0][0])
	assert.EqualValues(t, "false", records[0][1])
}

func TestQueryInterfaceNoParam(t *testing.T) {
	type GetVar5 struct {
		Uuid int64 `xorm:"pk"`
		Msg  bool
	}

	assert.NoError(t, PrepareScheme(&GetVar5{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	var data = GetVar5{
		Uuid: int64(1),
		Msg:  false,
	}
	_, err = engine.Insert(data)
	assert.NoError(t, err)

	records, err := engine.Table("get_var5").Cols("uuid", "msg").Limit(1).QueryInterface()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(records))
	/* 	assert.EqualValues(t, "1", string(records[0]["uuid"].([]byte)))
	   	assert.EqualValues(t, "false", string(records[0]["msg"].([]byte))) */
	assert.EqualValues(t, 1, records[0]["uuid"].(int64))
	assert.EqualValues(t, false, records[0]["msg"].(bool))

	records, err = engine.Table("get_var5").Cols("uuid", "msg").Where(builder.Eq{"`uuid`": int64(1)}).QueryInterface()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(records))
	/* 	assert.EqualValues(t, "1", string(records[0]["uuid"].([]byte)))
	   	assert.EqualValues(t, "false", string(records[0]["msg"].([]byte))) */
	assert.EqualValues(t, 1, records[0]["uuid"].(int64))
	assert.EqualValues(t, false, records[0]["msg"].(bool))
}

func TestQueryWithBuilder(t *testing.T) {
	type QueryWithBuilder struct {
		Uuid    int64  `xorm:"pk"`
		Msg     string `xorm:"varchar(255)"`
		Age     int
		Money   float64
		Created time.Time `xorm:"created"`
	}

	assert.NoError(t, PrepareScheme(&QueryWithBuilder{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	var q = QueryWithBuilder{
		Uuid:  int64(1),
		Msg:   "message",
		Age:   20,
		Money: 3000,
	}
	_, err = engine.Insert(&q)
	assert.NoError(t, err)

	assertResult := func(t *testing.T, results []map[string][]byte) {
		assert.EqualValues(t, 1, len(results))
		id, err := strconv.ParseInt(string(results[0]["uuid"]), 10, 64)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, id)
		assert.Equal(t, "message", string(results[0]["msg"]))

		age, err := strconv.Atoi(string(results[0]["age"]))
		assert.NoError(t, err)
		assert.EqualValues(t, 20, age)

		money, err := strconv.ParseFloat(string(results[0]["money"]), 32)
		assert.NoError(t, err)
		assert.EqualValues(t, 3000, money)
	}

	results, err := engine.Query(builder.Select("*").From(engine.Quote(engine.TableName("query_with_builder", true))))
	assert.NoError(t, err)
	assertResult(t, results)
}

func TestJoinWithSubQuery(t *testing.T) {
	type JoinWithSubQuery1 struct {
		Uuid     int64  `xorm:"pk"`
		Msg      string `xorm:"varchar(255)"`
		DepartId int64
		Money    float64
	}

	type JoinWithSubQueryDepart struct {
		Uuid int64 `xorm:"pk"`
		Name string
	}

	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)

	assert.NoError(t, engine.NewSession().DropTable(new(JoinWithSubQuery1)))
	assert.NoError(t, engine.NewSession().DropTable(new(JoinWithSubQueryDepart)))
	assert.NoError(t, engine.Sync(new(JoinWithSubQuery1), new(JoinWithSubQueryDepart)))

	var depart = JoinWithSubQueryDepart{
		Uuid: int64(1),
		Name: "depart1",
	}
	_, err = engine.Insert(&depart)
	assert.NoError(t, err)

	var q = JoinWithSubQuery1{
		Uuid:     int64(1),
		Msg:      "message",
		DepartId: depart.Uuid,
		Money:    3000,
	}

	_, err = engine.Insert(&q)
	assert.NoError(t, err)

	tbName := engine.Quote(engine.TableName("join_with_sub_query_depart", true))
	var querys []JoinWithSubQuery1
	err = engine.
		Table("join_with_sub_query1").
		Alias("jq1").
		Cols("jq1.uuid as uuid", "jq1.msg as msg", "jq1.depart_id as depart_id", "jq1.money as money").
		Join("INNER",
			builder.Select("`uuid`").From(tbName),
			"`join_with_sub_query_depart`.`uuid` = `jq1`.`depart_id`").
		Find(&querys)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(querys))
	assert.EqualValues(t, q, querys[0])

	querys = make([]JoinWithSubQuery1, 0, 1)
	err = engine.
		Table("join_with_sub_query1").
		Alias("jq1").
		Cols("jq1.uuid as uuid", "jq1.msg as msg", "jq1.depart_id as depart_id", "jq1.money as money").
		Join("INNER", "(SELECT `uuid` FROM "+tbName+") `a`", "`a`.`uuid` = `jq1`.`depart_id`").
		Find(&querys)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(querys))
	assert.EqualValues(t, q, querys[0])
}

func TestQueryStringWithLimit(t *testing.T) {
	type QueryWithLimit struct {
		Uuid     int64  `xorm:"pk"`
		Msg      string `xorm:"varchar(255)"`
		DepartId int64
		Money    float64
	}

	assert.NoError(t, PrepareScheme(&QueryWithLimit{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	data, err := engine.Table("query_with_limit").Limit(20, 20).QueryString()
	assert.NoError(t, err)
	assert.EqualValues(t, 0, len(data))
}
