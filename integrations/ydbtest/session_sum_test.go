package ydb

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func isFloatEq(i, j float64, precision int) bool {
	return fmt.Sprintf("%."+strconv.Itoa(precision)+"f", i) == fmt.Sprintf("%."+strconv.Itoa(precision)+"f", j)
}

func TestSum(t *testing.T) {
	type SumStruct struct {
		Int   int64 `xorm:"pk"`
		Float float32
	}

	assert.NoError(t, PrepareScheme(&SumStruct{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	var (
		cases = []SumStruct{
			{int64(1), 6.2},
			{int64(2), 5.3},
			{int64(92), -0.2},
		}
	)

	var i int64
	var f float32
	for _, v := range cases {
		i += int64(v.Int)
		f += v.Float
	}

	_, err = engine.Insert(cases)
	assert.NoError(t, err)

	colInt := engine.GetColumnMapper().Obj2Table("Int")
	colFloat := engine.GetColumnMapper().Obj2Table("Float")

	sumInt, err := engine.Sum(new(SumStruct), colInt)
	assert.NoError(t, err)
	assert.EqualValues(t, int64(sumInt), i)

	sumFloat, err := engine.Sum(new(SumStruct), colFloat)
	assert.NoError(t, err)
	assert.Condition(t, func() bool {
		return isFloatEq(sumFloat, float64(f), 2)
	})

	sums, err := engine.Sums(new(SumStruct), colInt, colFloat)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, len(sums))
	assert.EqualValues(t, i, int64(sums[0]))
	assert.Condition(t, func() bool {
		return isFloatEq(sums[1], float64(f), 2)
	})

	sumsInt, err := engine.SumsInt(new(SumStruct), colInt)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(sumsInt))
	assert.EqualValues(t, i, int64(sumsInt[0]))
}

type SumStructWithTableName struct {
	Int   int64 `xorm:"pk"`
	Float float32
}

func (s SumStructWithTableName) TableName() string {
	return "sum_struct_with_table_name_1"
}

func TestSumWithTableName(t *testing.T) {
	assert.NoError(t, PrepareScheme(&SumStructWithTableName{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	var (
		cases = []SumStructWithTableName{
			{int64(1), 6.2},
			{int64(2), 5.3},
			{int64(92), -0.2},
		}
	)

	var i int64
	var f float32
	for _, v := range cases {
		i += int64(v.Int)
		f += v.Float
	}

	_, err = engine.Insert(cases)
	assert.NoError(t, err)

	colInt := engine.GetColumnMapper().Obj2Table("Int")
	colFloat := engine.GetColumnMapper().Obj2Table("Float")

	sumInt, err := engine.Sum(new(SumStructWithTableName), colInt)
	assert.NoError(t, err)
	assert.EqualValues(t, int64(sumInt), i)

	sumFloat, err := engine.Sum(new(SumStructWithTableName), colFloat)
	assert.NoError(t, err)
	assert.Condition(t, func() bool {
		return isFloatEq(sumFloat, float64(f), 2)
	})

	sums, err := engine.Sums(new(SumStructWithTableName), colInt, colFloat)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, len(sums))
	assert.EqualValues(t, i, int64(sums[0]))
	assert.Condition(t, func() bool {
		return isFloatEq(sums[1], float64(f), 2)
	})

	sumsInt, err := engine.SumsInt(new(SumStructWithTableName), colInt)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(sumsInt))
	assert.EqualValues(t, i, int64(sumsInt[0]))
}

func TestSumCustomColumn(t *testing.T) {
	type SumStruct2 struct {
		Int   int64 `xorm:"pk"`
		Float float32
	}

	assert.NoError(t, PrepareScheme(&SumStruct2{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)

	var (
		cases = []SumStruct2{
			{int64(1), 6.2},
			{int64(2), 5.3},
			{int64(92), -0.2},
		}
	)

	_, err = engine.Insert(cases)
	assert.NoError(t, err)

	sumInt, err := engine.Sum(new(SumStruct2),
		"CASE WHEN `int` <= 2 THEN `int` ELSE 0 END")
	assert.NoError(t, err)
	assert.EqualValues(t, 3, int64(sumInt))
}
