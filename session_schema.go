// Copyright 2016 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xorm

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"xorm.io/xorm/dialects"
	"xorm.io/xorm/internal/utils"
	"xorm.io/xorm/schemas"
)

// Ping test if database is ok
func (session *Session) Ping() error {
	if session.isAutoClose {
		defer session.Close()
	}

	session.engine.logger.Infof("PING DATABASE %v", session.engine.DriverName())
	return session.DB().PingContext(session.ctx)
}

// CreateTable create a table according a bean
func (session *Session) CreateTable(bean interface{}) error {
	if session.isAutoClose {
		defer session.Close()
	}

	return session.createTable(bean)
}

func (session *Session) createTable(bean interface{}) error {
	if err := session.statement.SetRefBean(bean); err != nil {
		return err
	}

	session.statement.RefTable.StoreEngine = session.statement.StoreEngine
	session.statement.RefTable.Charset = session.statement.Charset
	tableName := session.statement.TableName()
	refTable := session.statement.RefTable
	if refTable.AutoIncrement != "" && session.engine.dialect.Features().AutoincrMode == dialects.SequenceAutoincrMode {
		sqlStr, err := session.engine.dialect.CreateSequenceSQL(context.Background(), session.engine.db, utils.SeqName(tableName))
		if err != nil {
			return err
		}
		if _, err := session.exec(sqlStr); err != nil {
			return err
		}
	}

	sqlStr, _, err := session.engine.dialect.CreateTableSQL(context.Background(), session.engine.db, refTable, tableName)
	if err != nil {
		return err
	}
	if _, err := session.exec(sqlStr); err != nil {
		return err
	}

	return nil
}

// CreateIndexes create indexes
func (session *Session) CreateIndexes(bean interface{}) error {
	if session.isAutoClose {
		defer session.Close()
	}

	return session.createIndexes(bean)
}

func (session *Session) createIndexes(bean interface{}) error {
	if err := session.statement.SetRefBean(bean); err != nil {
		return err
	}

	sqls := session.statement.GenIndexSQL()
	for _, sqlStr := range sqls {
		_, err := session.exec(sqlStr)
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateUniques create uniques
func (session *Session) CreateUniques(bean interface{}) error {
	if session.isAutoClose {
		defer session.Close()
	}
	return session.createUniques(bean)
}

func (session *Session) createUniques(bean interface{}) error {
	if err := session.statement.SetRefBean(bean); err != nil {
		return err
	}

	sqls := session.statement.GenUniqueSQL()
	for _, sqlStr := range sqls {
		_, err := session.exec(sqlStr)
		if err != nil {
			return err
		}
	}
	return nil
}

// DropIndexes drop indexes
func (session *Session) DropIndexes(bean interface{}) error {
	if session.isAutoClose {
		defer session.Close()
	}

	return session.dropIndexes(bean)
}

func (session *Session) dropIndexes(bean interface{}) error {
	if err := session.statement.SetRefBean(bean); err != nil {
		return err
	}

	sqls := session.statement.GenDelIndexSQL()
	for _, sqlStr := range sqls {
		_, err := session.exec(sqlStr)
		if err != nil {
			return err
		}
	}
	return nil
}

// DropTable drop table will drop table if exist, if drop failed, it will return error
func (session *Session) DropTable(beanOrTableName interface{}) error {
	if session.isAutoClose {
		defer session.Close()
	}

	return session.dropTable(beanOrTableName)
}

func (session *Session) dropTable(beanOrTableName interface{}) error {
	tableName := session.engine.TableName(beanOrTableName)
	sqlStr, checkIfExist := session.engine.dialect.DropTableSQL(session.engine.TableName(tableName, true))
	if !checkIfExist {
		exist, err := session.engine.dialect.IsTableExist(session.getQueryer(), session.ctx, tableName)
		if err != nil {
			return err
		}
		checkIfExist = exist
	}

	if !checkIfExist {
		return nil
	}
	if _, err := session.exec(sqlStr); err != nil {
		return err
	}

	// !datbeohbbh! above process is enough for ydb
	if session.engine.dialect.URI().DBType == schemas.YDB {
		return nil
	}

	if session.engine.dialect.Features().AutoincrMode == dialects.IncrAutoincrMode {
		return nil
	}

	var seqName = utils.SeqName(tableName)
	exist, err := session.engine.dialect.IsSequenceExist(session.ctx, session.getQueryer(), seqName)
	if err != nil {
		return err
	}
	if !exist {
		return nil
	}

	sqlStr, err = session.engine.dialect.DropSequenceSQL(seqName)
	if err != nil {
		return err
	}
	_, err = session.exec(sqlStr)
	return err
}

// IsTableExist if a table is exist
func (session *Session) IsTableExist(beanOrTableName interface{}) (bool, error) {
	if session.isAutoClose {
		defer session.Close()
	}

	tableName := session.engine.TableName(beanOrTableName)

	return session.isTableExist(tableName)
}

func (session *Session) isTableExist(tableName string) (bool, error) {
	return session.engine.dialect.IsTableExist(session.getQueryer(), session.ctx, tableName)
}

// IsTableEmpty if table have any records
func (session *Session) IsTableEmpty(bean interface{}) (bool, error) {
	if session.isAutoClose {
		defer session.Close()
	}
	return session.isTableEmpty(session.engine.TableName(bean))
}

func (session *Session) isTableEmpty(tableName string) (bool, error) {
	var total int64
	sqlStr := fmt.Sprintf("select count(*) from %s", session.engine.Quote(session.engine.TableName(tableName, true)))
	err := session.queryRow(sqlStr).Scan(&total)
	if err != nil {
		if err == sql.ErrNoRows {
			err = nil
		}
		return true, err
	}

	return total == 0, nil
}

func (session *Session) addColumn(colName string) error {
	col := session.statement.RefTable.GetColumn(colName)
	sql := session.engine.dialect.AddColumnSQL(session.statement.TableName(), col)
	_, err := session.exec(sql)
	return err
}

func (session *Session) addIndex(tableName, idxName string) error {
	index := session.statement.RefTable.Indexes[idxName]
	sqlStr := session.engine.dialect.CreateIndexSQL(tableName, index)
	_, err := session.exec(sqlStr)
	return err
}

func (session *Session) addUnique(tableName, uqeName string) error {
	index := session.statement.RefTable.Indexes[uqeName]
	sqlStr := session.engine.dialect.CreateIndexSQL(tableName, index)
	_, err := session.exec(sqlStr)
	return err
}

// Sync2 synchronize structs to database tables
// Depricated
func (session *Session) Sync2(beans ...interface{}) error {
	return session.Sync(beans...)
}

// !datbeohbbh! Sync synchronize structs to database tables for YDB
//  1. Speed up look up for table name, column name, index name
//  2. In YDB compare table name by strings.EqualFold is not true.
//     A = "/local/my_table"
//     B = "/local/My_table"
//     A and B are not same table
func (session *Session) ydbSync(beans ...interface{}) error {
	var (
		engine  = session.engine
		dialect = engine.Dialect()
	)

	if session.isAutoClose {
		session.isAutoClose = false
		defer session.Close()
	}

	tables, err := dialect.GetTables(session.getQueryer(), session.ctx)
	if err != nil {
		return err
	}

	tablesMap := make(map[string]*schemas.Table)
	for _, table := range tables {
		tablesMap[table.Name] = table
	}

	session.autoResetStatement = false
	defer func() {
		session.autoResetStatement = true
		session.resetStatement()
	}()

	ydbIsEqualIndexes := func(lhsIndex, rhsIndex *schemas.Index) bool {
		if lhsIndex.Type != rhsIndex.Type {
			return false
		}
		if len(lhsIndex.Cols) != len(rhsIndex.Cols) {
			return false
		}
		lhsColsMap := make(map[string]bool)
		for _, col := range lhsIndex.Cols {
			lhsColsMap[col] = true
		}
		for _, col := range rhsIndex.Cols {
			if _, has := lhsColsMap[col]; !has {
				return false
			}
		}
		return true
	}

	for _, bean := range beans {
		beanVal := utils.ReflectValue(bean)
		curTable, err := engine.tagParser.ParseWithCache(beanVal)
		if err != nil {
			return err
		}

		var curTableName string
		if len(session.statement.AltTableName) > 0 {
			curTableName = session.statement.AltTableName
		} else {
			curTableName = engine.TableName(bean)
		}
		curTableName = path.Join(dialect.URI().DBName, curTableName)

		var oriTable *schemas.Table
		if _, has := tablesMap[curTableName]; has {
			oriTable = tablesMap[curTableName]
		}

		// current bean is new table
		if oriTable == nil {
			err = session.createTable(bean)
			if err != nil {
				return err
			}
			// !datbeohbbh! update `tablesMap` so that next beans can know about new table
			tablesMap[curTableName] = curTable
			continue
		}

		if err = engine.loadTableInfo(oriTable); err != nil {
			return err
		}

		// set ref to currently sync table
		session.statement.RefTable = curTable
		session.statement.SetTableName(curTableName)

		// sync columns
		oriColumnsMap := make(map[string]*schemas.Column)
		for _, column := range oriTable.Columns() {
			oriColumnsMap[column.Name] = column
		}

		for _, curColumn := range curTable.Columns() {
			var oriColumn *schemas.Column
			if _, has := oriColumnsMap[curColumn.Name]; has {
				oriColumn = oriColumnsMap[curColumn.Name]
			}
			if oriColumn == nil {
				if err = session.addColumn(curColumn.Name); err != nil {
					return err
				}
				continue
			}
			curColumnType := dialect.SQLType(curColumn)
			oriColumnType := dialect.SQLType(oriColumn)
			// !datbeohbbh! YDB does not support modify column type.
			if curColumnType != oriColumnType {
				engine.logger.Warnf("Table `%s` column `%s` db type is `%s`, struct type is `%s`\n",
					curTableName, curColumn.Name, oriColumnType, curColumnType)
			}
		}

		// !datbeohbbh! https://ydb.tech/en/docs/yql/reference/syntax/create_table#secondary_index
		// Currently, just support type of indexes is SYNC.
		// Reason is that I can not (or don't know) how to get infor about indexes type from sdk side.

		// sync indexes
		commonIndexes := make(map[string]bool)
		toAddCurIndexes := make([]string, 0)
		for curIndexName, curIndex := range curTable.Indexes {
			hasCommon := false
			for oriIndexName, oriIndex := range oriTable.Indexes {
				if ydbIsEqualIndexes(oriIndex, curIndex) {
					commonIndexes[oriIndexName] = true
					hasCommon = true
					break
				}
			}

			if !hasCommon {
				toAddCurIndexes = append(toAddCurIndexes, curIndexName)
			}
		}

		for oriIndexName, oriIndex := range oriTable.Indexes {
			if _, isCommon := commonIndexes[oriIndexName]; !isCommon {
				sql := dialect.DropIndexSQL(curTableName, oriIndex)
				if _, err = session.exec(sql); err != nil {
					return err
				}
			}
		}

		for _, curIndexName := range toAddCurIndexes {
			if err = session.addIndex(curTableName, curIndexName); err != nil {
				return err
			}
		}

		for _, columnName := range oriTable.ColumnsSeq() {
			if curTable.GetColumn(columnName) == nil {
				engine.logger.Warnf("Table %s has column %s but struct has not related field",
					engine.TableName(oriTable.Name), columnName)
			}
		}
	}

	return nil
}

// Sync synchronize structs to database tables
func (session *Session) Sync(beans ...interface{}) error {
	engine := session.engine

	switch engine.dialect.URI().DBType {
	case schemas.YDB:
		return session.ydbSync(beans...)
	}

	if session.isAutoClose {
		session.isAutoClose = false
		defer session.Close()
	}

	tables, err := engine.dialect.GetTables(session.getQueryer(), session.ctx)
	if err != nil {
		return err
	}

	session.autoResetStatement = false
	defer func() {
		session.autoResetStatement = true
		session.resetStatement()
	}()

	for _, bean := range beans {
		v := utils.ReflectValue(bean)
		table, err := engine.tagParser.ParseWithCache(v)
		if err != nil {
			return err
		}
		var tbName string
		if len(session.statement.AltTableName) > 0 {
			tbName = session.statement.AltTableName
		} else {
			tbName = engine.TableName(bean)
		}
		tbNameWithSchema := engine.tbNameWithSchema(tbName)

		var oriTable *schemas.Table
		for _, tb := range tables {
			if strings.EqualFold(engine.tbNameWithSchema(tb.Name), engine.tbNameWithSchema(tbName)) {
				oriTable = tb
				break
			}
		}

		// this is a new table
		if oriTable == nil {
			err = session.StoreEngine(session.statement.StoreEngine).createTable(bean)
			if err != nil {
				return err
			}

			err = session.createUniques(bean)
			if err != nil {
				return err
			}

			err = session.createIndexes(bean)
			if err != nil {
				return err
			}
			continue
		}

		// this will modify an old table
		if err = engine.loadTableInfo(oriTable); err != nil {
			return err
		}

		// check columns
		for _, col := range table.Columns() {
			var oriCol *schemas.Column
			for _, col2 := range oriTable.Columns() {
				if strings.EqualFold(col.Name, col2.Name) {
					oriCol = col2
					break
				}
			}

			// column is not exist on table
			if oriCol == nil {
				session.statement.RefTable = table
				session.statement.SetTableName(tbNameWithSchema)
				if err = session.addColumn(col.Name); err != nil {
					return err
				}
				continue
			}

			err = nil
			expectedType := engine.dialect.SQLType(col)
			curType := engine.dialect.SQLType(oriCol)
			if expectedType != curType {
				if expectedType == schemas.Text &&
					strings.HasPrefix(curType, schemas.Varchar) {
					// currently only support mysql & postgres
					if engine.dialect.URI().DBType == schemas.MYSQL ||
						engine.dialect.URI().DBType == schemas.POSTGRES {
						engine.logger.Infof("Table %s column %s change type from %s to %s\n",
							tbNameWithSchema, col.Name, curType, expectedType)
						_, err = session.exec(engine.dialect.ModifyColumnSQL(tbNameWithSchema, col))
					} else {
						engine.logger.Warnf("Table %s column %s db type is %s, struct type is %s\n",
							tbNameWithSchema, col.Name, curType, expectedType)
					}
				} else if strings.HasPrefix(curType, schemas.Varchar) && strings.HasPrefix(expectedType, schemas.Varchar) {
					if engine.dialect.URI().DBType == schemas.MYSQL {
						if oriCol.Length < col.Length {
							engine.logger.Infof("Table %s column %s change type from varchar(%d) to varchar(%d)\n",
								tbNameWithSchema, col.Name, oriCol.Length, col.Length)
							_, err = session.exec(engine.dialect.ModifyColumnSQL(tbNameWithSchema, col))
						}
					}
				} else {
					if !(strings.HasPrefix(curType, expectedType) && curType[len(expectedType)] == '(') {
						if !strings.EqualFold(schemas.SQLTypeName(curType), engine.dialect.Alias(schemas.SQLTypeName(expectedType))) {
							engine.logger.Warnf("Table %s column %s db type is %s, struct type is %s",
								tbNameWithSchema, col.Name, curType, expectedType)
						}
					}
				}
			} else if expectedType == schemas.Varchar {
				if engine.dialect.URI().DBType == schemas.MYSQL {
					if oriCol.Length < col.Length {
						engine.logger.Infof("Table %s column %s change type from varchar(%d) to varchar(%d)\n",
							tbNameWithSchema, col.Name, oriCol.Length, col.Length)
						_, err = session.exec(engine.dialect.ModifyColumnSQL(tbNameWithSchema, col))
					}
				}
			} else if col.Comment != oriCol.Comment {
				_, err = session.exec(engine.dialect.ModifyColumnSQL(tbNameWithSchema, col))
			}

			if col.Default != oriCol.Default {
				switch {
				case col.IsAutoIncrement: // For autoincrement column, don't check default
				case (col.SQLType.Name == schemas.Bool || col.SQLType.Name == schemas.Boolean) &&
					((strings.EqualFold(col.Default, "true") && oriCol.Default == "1") ||
						(strings.EqualFold(col.Default, "false") && oriCol.Default == "0")):
				default:
					engine.logger.Warnf("Table %s Column %s db default is %s, struct default is %s",
						tbName, col.Name, oriCol.Default, col.Default)
				}
			}
			if col.Nullable != oriCol.Nullable {
				engine.logger.Warnf("Table %s Column %s db nullable is %v, struct nullable is %v",
					tbName, col.Name, oriCol.Nullable, col.Nullable)
			}

			if err != nil {
				return err
			}
		}

		var foundIndexNames = make(map[string]bool)
		var addedNames = make(map[string]*schemas.Index)

		for name, index := range table.Indexes {
			var oriIndex *schemas.Index
			for name2, index2 := range oriTable.Indexes {
				if index.Equal(index2) {
					oriIndex = index2
					foundIndexNames[name2] = true
					break
				}
			}

			if oriIndex != nil {
				if oriIndex.Type != index.Type {
					sql := engine.dialect.DropIndexSQL(tbNameWithSchema, oriIndex)
					_, err = session.exec(sql)
					if err != nil {
						return err
					}
					oriIndex = nil
				}
			}

			if oriIndex == nil {
				addedNames[name] = index
			}
		}

		for name2, index2 := range oriTable.Indexes {
			if _, ok := foundIndexNames[name2]; !ok {
				sql := engine.dialect.DropIndexSQL(tbNameWithSchema, index2)
				_, err = session.exec(sql)
				if err != nil {
					return err
				}
			}
		}

		for name, index := range addedNames {
			if index.Type == schemas.UniqueType {
				session.statement.RefTable = table
				session.statement.SetTableName(tbNameWithSchema)
				err = session.addUnique(tbNameWithSchema, name)
			} else if index.Type == schemas.IndexType {
				session.statement.RefTable = table
				session.statement.SetTableName(tbNameWithSchema)
				err = session.addIndex(tbNameWithSchema, name)
			}
			if err != nil {
				return err
			}
		}

		// check all the columns which removed from struct fields but left on database tables.
		for _, colName := range oriTable.ColumnsSeq() {
			if table.GetColumn(colName) == nil {
				engine.logger.Warnf("Table %s has column %s but struct has not related field", engine.TableName(oriTable.Name, true), colName)
			}
		}
	}

	return nil
}

// ImportFile SQL DDL file
func (session *Session) ImportFile(ddlPath string) ([]sql.Result, error) {
	file, err := os.Open(ddlPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return session.Import(file)
}

// Import SQL DDL from io.Reader
func (session *Session) Import(r io.Reader) ([]sql.Result, error) {
	var (
		results       []sql.Result
		lastError     error
		inSingleQuote bool
		startComment  bool
	)

	scanner := bufio.NewScanner(r)
	semiColSpliter := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		var oriInSingleQuote = inSingleQuote
		for i, b := range data {
			if startComment {
				if b == '\n' {
					startComment = false
				}
			} else {
				if !inSingleQuote && i > 0 && data[i-1] == '-' && data[i] == '-' {
					startComment = true
					continue
				}

				if b == '\'' {
					inSingleQuote = !inSingleQuote
				}
				if !inSingleQuote && b == ';' {
					return i + 1, data[0:i], nil
				}
			}
		}
		// If we're at EOF, we have a final, non-terminated line. Return it.
		if atEOF {
			return len(data), data, nil
		}
		inSingleQuote = oriInSingleQuote
		// Request more data.
		return 0, nil, nil
	}

	scanner.Split(semiColSpliter)

	for scanner.Scan() {
		query := strings.Trim(scanner.Text(), " \t\n\r")
		if len(query) > 0 {
			result, err := session.Exec(query)
			if err != nil {
				return nil, err
			}
			results = append(results, result)
		}
	}

	return results, lastError
}
