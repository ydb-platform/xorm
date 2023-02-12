// Copyright 2016 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"

	"xorm.io/xorm/core"
	"xorm.io/xorm/internal/statements"
	"xorm.io/xorm/retry"
	"xorm.io/xorm/schemas"
)

func (session *Session) queryPreprocess(sqlStr *string, paramStr ...interface{}) {
	for _, filter := range session.engine.dialect.Filters() {
		*sqlStr = filter.Do(*sqlStr)
	}

	switch session.engine.dialect.URI().DBType {
	case schemas.YDB:
		declareSection := ""
		for _, filter := range session.engine.dialect.Filters() {
			if f, ok := filter.(interface {
				GenerateDeclareSection(...interface{}) string
			}); ok {
				declareSection += f.GenerateDeclareSection(paramStr...)
			}
		}
		*sqlStr = declareSection + *sqlStr
	default:
	}

	session.lastSQL = *sqlStr
	session.lastSQLArgs = paramStr
}

func (session *Session) queryRows(sqlStr string, args ...interface{}) (*core.Rows, error) {
	defer session.resetStatement()
	if session.statement.LastError != nil {
		return nil, session.statement.LastError
	}

	session.queryPreprocess(&sqlStr, args...)

	session.lastSQL = sqlStr
	session.lastSQLArgs = args

	if session.isAutoCommit {
		var db *core.DB
		if session.sessionType == groupSession && strings.EqualFold(strings.TrimSpace(sqlStr)[:6], "select") && !session.statement.IsForUpdate {
			db = session.engine.engineGroup.Slave().DB()
		} else {
			db = session.DB()
		}

		if session.prepareStmt {
			// don't clear stmt since session will cache them
			stmt, err := session.doPrepare(db, sqlStr)
			if err != nil {
				return nil, err
			}

			return stmt.QueryContext(session.ctx, args...)
		}

		return db.QueryContext(session.ctx, sqlStr, args...)
	}

	if session.prepareStmt {
		stmt, err := session.doPrepareTx(sqlStr)
		if err != nil {
			return nil, err
		}

		return stmt.QueryContext(session.ctx, args...)
	}

	return session.tx.QueryContext(session.ctx, sqlStr, args...)
}

func (session *Session) queryRow(sqlStr string, args ...interface{}) *core.Row {
	return core.NewRow(session.queryRows(sqlStr, args...))
}

// Query runs a raw sql and return records as []map[string][]byte
func (session *Session) Query(sqlOrArgs ...interface{}) ([]map[string][]byte, error) {
	if session.isAutoClose {
		defer session.Close()
	}

	sqlStr, args, err := session.statement.GenQuerySQL(sqlOrArgs...)
	if err != nil {
		return nil, err
	}

	rows, err := session.queryRows(sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return session.engine.scanByteMaps(rows)
}

// QueryString runs a raw sql and return records as []map[string]string
func (session *Session) QueryString(sqlOrArgs ...interface{}) ([]map[string]string, error) {
	if session.isAutoClose {
		defer session.Close()
	}

	sqlStr, args, err := session.statement.GenQuerySQL(sqlOrArgs...)
	if err != nil {
		return nil, err
	}

	rows, err := session.queryRows(sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return session.engine.ScanStringMaps(rows)
}

// QuerySliceString runs a raw sql and return records as [][]string
func (session *Session) QuerySliceString(sqlOrArgs ...interface{}) ([][]string, error) {
	if session.isAutoClose {
		defer session.Close()
	}

	sqlStr, args, err := session.statement.GenQuerySQL(sqlOrArgs...)
	if err != nil {
		return nil, err
	}

	rows, err := session.queryRows(sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return session.engine.ScanStringSlices(rows)
}

// QueryInterface runs a raw sql and return records as []map[string]interface{}
func (session *Session) QueryInterface(sqlOrArgs ...interface{}) ([]map[string]interface{}, error) {
	if session.isAutoClose {
		defer session.Close()
	}

	sqlStr, args, err := session.statement.GenQuerySQL(sqlOrArgs...)
	if err != nil {
		return nil, err
	}

	rows, err := session.queryRows(sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return session.engine.ScanInterfaceMaps(rows)
}

func (session *Session) exec(sqlStr string, args ...interface{}) (sql.Result, error) {
	defer session.resetStatement()

	session.queryPreprocess(&sqlStr, args...)

	session.lastSQL = sqlStr
	session.lastSQLArgs = args

	if !session.isAutoCommit {
		if session.prepareStmt {
			stmt, err := session.doPrepareTx(sqlStr)
			if err != nil {
				return nil, err
			}
			return stmt.ExecContext(session.ctx, args...)
		}
		switch session.engine.dialect.URI().DBType {
		case schemas.YDB:
			dialect := session.engine.dialect
			err := retry.Retry(session.ctx, dialect.IsRetryable, func(ctx context.Context) (err error) {
				if !session.IsInTx() {
					if err = session.Begin(); err != nil {
						return err
					}
				}

				defer func() {
					_ = session.Rollback()
				}()

				_, err = session.tx.ExecContext(ctx, sqlStr, args...)

				if err != nil {
					return err
				}

				if err = session.Commit(); err != nil {
					return err
				}

				return nil
			},
				retry.WithID("ydb-auto-commit"),
				retry.WithIdempotent(true),
			)

			if err != nil {
				return nil, err
			}
			return driver.ResultNoRows, session.Begin()
		default:
			return session.tx.ExecContext(session.ctx, sqlStr, args...)
		}
	}

	if session.prepareStmt {
		stmt, err := session.doPrepare(session.DB(), sqlStr)
		if err != nil {
			return nil, err
		}
		return stmt.ExecContext(session.ctx, args...)
	}

	return session.DB().ExecContext(session.ctx, sqlStr, args...)
}

// Exec raw sql
func (session *Session) Exec(sqlOrArgs ...interface{}) (sql.Result, error) {
	if session.isAutoClose {
		defer session.Close()
	}

	if len(sqlOrArgs) == 0 {
		return nil, ErrUnSupportedType
	}

	sqlStr, args, err := session.statement.ConvertSQLOrArgs(sqlOrArgs...)
	if err != nil {
		return nil, err
	}

	return session.exec(sqlStr, args...)
}
