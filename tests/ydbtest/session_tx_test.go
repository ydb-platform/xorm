package ydb

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"xorm.io/xorm"
	"xorm.io/xorm/retry"
)

// !datbeohbbh! transactions concept
// https://ydb.tech/en/docs/concepts/transactions

func TestTx(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	userData := Users{
		Name: "Dat",
		Age:  22,
		Account: Account{
			UserID: sql.NullInt64{Int64: 1234, Valid: true},
			Number: "56789",
		},
	}

	session := engine.NewSession()
	defer session.Close()

	err = session.Begin()
	assert.NoError(t, err)

	before, err := session.Count(&userData)
	assert.NoError(t, err)

	_, err = session.Insert(&userData)
	if !assert.NoError(t, err) {
		session.Rollback()
	}

	err = session.Commit()
	assert.NoError(t, err)

	after, err := session.Count(&userData)
	assert.NoError(t, err)

	assert.Equal(t, after, before+1)
}

func TestMultipleTx(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	userDataA := Users{
		Name: "Dat",
		Age:  21,
		Account: Account{
			UserID: sql.NullInt64{Int64: 1234, Valid: true},
			Number: "56789",
		},
	}

	userDataB := Users{
		Name: "Dat",
		Age:  22,
		Account: Account{
			UserID: sql.NullInt64{Int64: 5678, Valid: true},
			Number: "102030",
		},
	}

	session := engine.NewSession()
	defer session.Close()

	err = session.Begin()
	assert.NoError(t, err)

	_, err = session.Insert(&userDataA)
	if !assert.NoError(t, err) {
		session.Rollback()
	}

	err = session.Commit()
	if !assert.NoError(t, err) {
		session.Rollback()
	}

	err = session.Begin()
	assert.NoError(t, err)

	_, err = session.Exec(
		fmt.Sprintf("INSERT INTO `%s` (name, age, user_id, number) VALUES (\"%s\", %d, %d, \"%s\")",
			(&Users{}).TableName(),
			userDataB.Name,
			userDataB.Age,
			userDataB.UserID.Int64,
			userDataB.Number))
	assert.NoError(t, err)
	if !assert.NoError(t, err) {
		session.Rollback()
	}

	err = session.Commit()
	if !assert.NoError(t, err) {
		session.Rollback()
	}

	after, err := session.Count(&Users{})
	assert.NoError(t, err)
	assert.Equal(t, after, int64(2))
}

func TestEngineTx(t *testing.T) {
	assert.NoError(t, PrepareScheme(&Users{}))

	engine, err := enginePool.GetDataQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	userDataA := Users{
		Name: "Dat",
		Age:  21,
		Account: Account{
			UserID: sql.NullInt64{Int64: 1234, Valid: true},
			Number: "56789",
		},
	}

	userDataB := Users{
		Name: "Dat",
		Age:  22,
		Account: Account{
			UserID: sql.NullInt64{Int64: 5678, Valid: true},
			Number: "102030",
		},
	}

	_, err = engine.Transaction(func(session *xorm.Session) (interface{}, error) {
		users := []*Users{&userDataA, &userDataB}
		_, err := session.Insert(&users)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	assert.NoError(t, err)

	_, err = engine.Transaction(func(session *xorm.Session) (interface{}, error) {
		_, err := session.Table(&Users{}).Delete(userDataA)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	assert.NoError(t, err)

	_, err = engine.Transaction(func(session *xorm.Session) (interface{}, error) {
		hasA, err := session.Exist(&userDataA)
		if err != nil {
			return nil, err
		}
		assert.False(t, hasA)

		hasB, err := session.Exist(&userDataB)
		if err != nil {
			return false, err
		}
		assert.True(t, hasB)

		return nil, nil
	})
	assert.NoError(t, err)
}

func TestDDLTx(t *testing.T) {
	engine, err := enginePool.GetSchemeQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	err = engine.DoTx(enginePool.ctx, func(ctx context.Context, session *xorm.Session) error {
		for _, bean := range []interface{}{
			&Users{},
			&Series{},
			&Seasons{},
			&Episodes{},
		} {
			if err := session.DropTable(bean); err != nil {
				return err
			}
			if err := session.CreateTable(bean); err != nil {
				return err
			}
		}

		return nil
	}, retry.WithIdempotent(true))

	assert.NoError(t, err)
}

func TestDDLTxSync(t *testing.T) {
	engine, err := enginePool.GetSchemeQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	err = engine.DoTx(enginePool.ctx, func(ctx context.Context, session *xorm.Session) error {
		for _, bean := range []interface{}{
			&Users{},
			&Series{},
			&Seasons{},
			&Episodes{},
		} {
			if err := session.DropTable(bean); err != nil {
				return err
			}
		}

		err := session.Sync(&Users{}, &Series{}, &Seasons{}, &Episodes{})
		return err
	}, retry.WithIdempotent(true))

	assert.NoError(t, err)
}

func TestInsertMulti2InterfaceTransaction(t *testing.T) {
	type Multi2InterfaceTransaction struct {
		ID         uint64 `xorm:"id pk"`
		Name       string
		Alias      string
		CreateTime sql.NullTime
		UpdateTime sql.NullTime
	}

	engine, err := enginePool.GetScriptQueryEngine()
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	assert.NoError(t, engine.Sync(&Multi2InterfaceTransaction{}))

	session := engine.NewSession()
	defer session.Close()

	err = session.Begin()
	assert.NoError(t, err)

	users := []interface{}{
		&Multi2InterfaceTransaction{ID: 1, Name: "a", Alias: "A"},
		&Multi2InterfaceTransaction{ID: 2, Name: "b", Alias: "B"},
		&Multi2InterfaceTransaction{ID: 3, Name: "c", Alias: "C"},
		&Multi2InterfaceTransaction{ID: 4, Name: "d", Alias: "D"},
	}
	_, err = session.Insert(&users)

	assert.NoError(t, err)

	assert.NotPanics(t, func() {
		err = session.Commit()
		assert.NoError(t, err)
	})
}