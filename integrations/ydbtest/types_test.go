package ydb

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"xorm.io/xorm/convert"
)

type Models struct {
	Log     *LogEntry `xorm:"BLOB"`
	ModelID string    `xorm:"pk 'model_id'" json:"model_id"`
}

type LogEntry struct {
	LogID     string    `json:"log_id,omitempty"`
	Name      string    `json:"name"`
	Data      string    `json:"data"`
	CreatedAt time.Time `json:"create_at"`
	UpdateAt  time.Time `json:"update_at"`
}

func (l *LogEntry) FromDB(data []byte) error {
	if data == nil {
		l = nil
		return nil
	}
	return json.Unmarshal(data, l)
}

func (l *LogEntry) ToDB() ([]byte, error) {
	if l == nil {
		return nil, nil
	}
	return json.MarshalIndent(l, "\t", "")
}

func TestConversionModels(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Models{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	m := Models{
		ModelID: "model_abc",
		Log: &LogEntry{
			LogID:     "log_abc",
			Name:      "abc",
			Data:      "xyz",
			CreatedAt: time.Now(),
			UpdateAt:  time.Now(),
		},
	}

	_, err = engine.Insert(&m)
	assert.NoError(t, err)

	var ret Models
	has, err := engine.Get(&ret)
	assert.True(t, has)
	assert.NoError(t, err)
	assert.EqualValues(t, m.ModelID, ret.ModelID)
	assert.EqualValues(t, m.Log.LogID, ret.Log.LogID)
	assert.EqualValues(t, m.Log.Name, ret.Log.Name)
	assert.EqualValues(t, m.Log.Data, ret.Log.Data)
	assert.EqualValues(t, m.Log.CreatedAt.Format(time.RFC3339Nano), ret.Log.CreatedAt.Format(time.RFC3339Nano))
	assert.EqualValues(t, m.Log.UpdateAt.Format(time.RFC3339Nano), ret.Log.UpdateAt.Format(time.RFC3339Nano))
}

func TestConversionModelsCond(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Models{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	m := make([]*Models, 0)
	for i := 0; i <= 10; i++ {
		m = append(m, &Models{
			ModelID: fmt.Sprintf("%d", i),
			Log: &LogEntry{
				LogID: fmt.Sprintf("log - %d", i),
				Name:  fmt.Sprintf("%d", i),
				Data:  fmt.Sprintf("%d", i),
			},
		})
	}

	_, err = engine.Insert(m)
	assert.NoError(t, err)

	res := make([]*Models, 0)
	err = engine.
		In("model_id", []string{"0", "1", "2", "3", "4", "5"}).
		Find(&res)
	assert.NoError(t, err)
	assert.ElementsMatch(t, m[:6], res)
}

func TestConversionModelsUpdate(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Models{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	m := make([]*Models, 0)
	for i := 0; i <= 10; i++ {
		m = append(m, &Models{
			ModelID: fmt.Sprintf("%d", i),
			Log: &LogEntry{
				LogID: fmt.Sprintf("log - %d", i),
				Name:  fmt.Sprintf("%d", i),
				Data:  fmt.Sprintf("%d", i),
			},
		})
	}

	_, err = engine.Insert(m)
	assert.NoError(t, err)

	_, err = engine.
		In("model_id", []string{"0", "1", "2", "3", "4", "5"}).
		Update(&Models{
			Log: &LogEntry{
				LogID: fmt.Sprintf("log - %d", 2023),
				Name:  fmt.Sprintf("%d", 2023),
				Data:  fmt.Sprintf("%d", 2023),
			},
		})
	assert.NoError(t, err)

	res := make([]*Models, 0)
	err = engine.
		In("model_id", []string{"0", "1", "2", "3", "4", "5"}).
		Find(&res)
	assert.NoError(t, err)
	for i := 0; i < 5; i++ {
		assert.EqualValues(t, &LogEntry{
			LogID: fmt.Sprintf("log - %d", 2023),
			Name:  fmt.Sprintf("%d", 2023),
			Data:  fmt.Sprintf("%d", 2023),
		}, res[i].Log)
	}
}

type MyDecimal big.Int

func (d *MyDecimal) FromDB(data []byte) error {
	i, _ := strconv.ParseInt(string(data), 10, 64)
	if d == nil {
		d = (*MyDecimal)(big.NewInt(i))
	} else {
		(*big.Int)(d).SetInt64(i)
	}
	return nil
}

func (d *MyDecimal) ToDB() ([]byte, error) {
	return []byte(fmt.Sprintf("%d", (*big.Int)(d).Int64())), nil
}

func (d *MyDecimal) AsBigInt() *big.Int {
	return (*big.Int)(d)
}

func (d *MyDecimal) AsInt64() int64 {
	return d.AsBigInt().Int64()
}

func TestDecimal(t *testing.T) {
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	type MyMoney struct {
		Uuid    int64 `xorm:"pk"`
		Account *MyDecimal
	}

	assert.NoError(t, PrepareScheme(&MyMoney{}))

	_, err = engine.Insert(&MyMoney{
		Account: (*MyDecimal)(big.NewInt(10000000000000000)),
	})
	assert.NoError(t, err)

	var m MyMoney
	has, err := engine.Get(&m)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.NotNil(t, m.Account)
	assert.EqualValues(t, 10000000000000000, m.Account.AsInt64())
}

type MyArray [20]byte

func (d *MyArray) FromDB(data []byte) error {
	for i, b := range data[:20] {
		(*d)[i] = b
	}
	return nil
}

func (d MyArray) ToDB() ([]byte, error) {
	return d[:], nil
}

func TestMyArray(t *testing.T) {
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	type MyArrayStruct struct {
		Uuid    int64 `xorm:"pk"`
		Content MyArray
	}

	assert.NoError(t, PrepareScheme(&MyArrayStruct{}))

	v := [20]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	_, err = engine.Insert(&MyArrayStruct{
		Content: v,
	})
	assert.NoError(t, err)

	var m MyArrayStruct
	has, err := engine.Get(&m)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.EqualValues(t, v, m.Content)
}

type Status struct {
	Name  string
	Color string
}

var (
	_          convert.Conversion = &Status{}
	Registered                    = Status{"Registered", "white"}
	Approved                      = Status{"Approved", "green"}
	Removed                       = Status{"Removed", "red"}
	Statuses                      = map[string]Status{
		Registered.Name: Registered,
		Approved.Name:   Approved,
		Removed.Name:    Removed,
	}
)

func (s *Status) FromDB(bytes []byte) error {
	if r, ok := Statuses[string(bytes)]; ok {
		*s = r
		return nil
	}
	return errors.New("no this data")
}

func (s *Status) ToDB() ([]byte, error) {
	return []byte(s.Name), nil
}

type UserCus struct {
	Uuid   int64 `xorm:"pk"`
	Name   string
	Status Status `xorm:"VARCHAR"`
}

func TestCustomType2(t *testing.T) {
	assert.NoError(t, PrepareScheme(&UserCus{}))
	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	session := engine.NewSession()
	defer session.Close()

	_, err = session.Insert(&UserCus{int64(1), "xlw", Registered})
	assert.NoError(t, err)

	user := UserCus{}
	exist, err := engine.ID(int64(1)).Get(&user)
	assert.NoError(t, err)
	assert.True(t, exist)

	users := make([]UserCus, 0)
	err = engine.Where("`"+engine.GetColumnMapper().Obj2Table("Status")+"` = ?", "Registered").Find(&users)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(users))
}
