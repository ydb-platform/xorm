package xorm

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"xorm.io/builder"
	"xorm.io/xorm/schemas"
)

func (session *Session) Replace(beans ...interface{}) (int64, error) {
	if session.engine.dialect.URI().DBType != schemas.YDB {
		return 0, errors.New("(*Session).Replace is only used for YDB")
	}

	var (
		affected int64
		err      error
	)

	if session.isAutoClose {
		defer session.Close()
	}

	session.autoResetStatement = false
	defer func() {
		session.autoResetStatement = true
		session.resetStatement()
	}()

	for _, bean := range beans {
		var cnt int64
		var err error
		switch v := bean.(type) {
		case *builder.Builder:
			cnt, err = session.replaceByFetchValues(v)
		case map[string]interface{}:
			cnt, err = session.replaceMapInterface(v)
		case []map[string]interface{}:
			cnt, err = session.replaceMultipleMapInterface(v)
		default:
			err = fmt.Errorf("REPLACE INTO does not support type: %s", reflect.TypeOf(v).String())
		}
		if err != nil {
			if session.engine.dialect.URI().DBType == schemas.YDB &&
				err.Error() == ErrRowAffectedUnsupported.Error() {
				err = nil
				continue
			}
			return affected, err
		}
		affected += cnt
	}

	return affected, err
}

func splitCmds(sql string) (string, string, error) {
	var declareSection, execCmd string
	sql = strings.TrimSpace(sql)

	cmds := strings.Split(sql, ";")
	if len(cmds) == 0 {
		return "", "", fmt.Errorf("builder generated empty SQL string")
	}

	for _, cmd := range cmds {
		if strings.HasPrefix(cmd, "DECLARE") {
			declareSection += cmd
			declareSection += ";"
		} else {
			execCmd += cmd
			execCmd += ";"
		}
	}
	return declareSection, execCmd, nil
}

func (session *Session) replaceByFetchValues(b *builder.Builder) (int64, error) {
	// !datbeohbbh! note: xorm/builder does not apply quote policy
	var (
		buf       strings.Builder
		tableName = session.statement.TableName()
		quoter    = session.engine.dialect.Quoter()
	)

	fetchSQL, args, err := b.ToSQL()
	if err != nil {
		return 0, err
	}

	for _, filter := range session.engine.dialect.Filters() {
		fetchSQL = filter.DoWithDeclare(fetchSQL, args...)
	}

	declareSection, execCmd, err := splitCmds(fetchSQL)
	if err != nil {
		return 0, nil
	}

	if _, err = buf.WriteString(declareSection); err != nil {
		return 0, err
	}

	if _, err = buf.WriteString(fmt.Sprintf("$fetchedData = %s", execCmd)); err != nil {
		return 0, err
	}

	if _, err = buf.WriteString(fmt.Sprintf("REPLACE INTO %s ( SELECT * FROM $fetchedData );", quoter.Quote(tableName))); err != nil {
		return 0, err
	}

	sqlStr := buf.String()

	res, err := session.exec(sqlStr, args...)
	if err != nil {
		return 0, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return affected, nil
}

func (session *Session) replaceMapInterface(m map[string]interface{}) (int64, error) {
	if len(m) == 0 {
		return 0, ErrParamsType
	}

	tableName := session.statement.TableName()
	if len(tableName) == 0 {
		return 0, ErrTableNotFound
	}

	columns := make([]string, 0, len(m))
	exprs := session.statement.ExprColumns
	for col := range m {
		if !exprs.IsColExist(col) {
			columns = append(columns, col)
		}
	}

	sort.Strings(columns)

	args := make([]interface{}, 0, len(m))
	for _, col := range columns {
		args = append(args, m[col])
	}

	return session.replaceMap(columns, args)
}

func (session *Session) replaceMultipleMapInterface(ms []map[string]interface{}) (int64, error) {
	if len(ms) == 0 {
		return 0, ErrNoElementsOnSlice
	}

	tableName := session.statement.TableName()
	if len(tableName) == 0 {
		return 0, ErrTableNotFound
	}

	columns := make([]string, 0, len(ms))
	exprs := session.statement.ExprColumns
	for col := range ms[0] {
		if !exprs.IsColExist(col) {
			columns = append(columns, col)
		}
	}

	sort.Strings(columns)

	argss := make([][]interface{}, 0, len(ms))
	for _, m := range ms {
		args := make([]interface{}, 0, len(m))
		for _, col := range columns {
			args = append(args, m[col])
		}
		argss = append(argss, args)
	}

	return session.replaceMultipleMap(columns, argss)
}

func (session *Session) replaceMap(columns []string, args []interface{}) (int64, error) {
	tableName := session.statement.TableName()
	if len(tableName) == 0 {
		return 0, ErrTableNotFound
	}

	sqlStr, args, err := session.genReplaceMapSQL(columns, args)
	if err != nil {
		return 0, err
	}

	sqlStr = session.engine.dialect.Quoter().Replace(sqlStr)
	// no cache

	res, err := session.exec(sqlStr, args...)
	if err != nil {
		return 0, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return affected, nil
}

func (session *Session) replaceMultipleMap(columns []string, argss [][]interface{}) (int64, error) {
	tableName := session.statement.TableName()
	if len(tableName) == 0 {
		return 0, ErrTableNotFound
	}

	sqlStr, args, err := session.genReplaceMultipleMapSQL(columns, argss)
	if err != nil {
		return 0, err
	}

	sqlStr = session.engine.dialect.Quoter().Replace(sqlStr)
	// no cache

	res, err := session.exec(sqlStr, args...)
	if err != nil {
		return 0, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return affected, nil
}

func (session *Session) genReplaceMapSQL(columns []string, args []interface{}) (string, []interface{}, error) {
	var (
		buf       = builder.NewWriter()
		exprs     = session.statement.ExprColumns
		tableName = session.statement.TableName()
		quoter    = session.engine.dialect.Quoter()
	)

	if _, err := buf.WriteString(fmt.Sprintf("REPLACE INTO %s (", quoter.Quote(tableName))); err != nil {
		return "", nil, err
	}

	if err := quoter.JoinWrite(buf.Builder, append(columns, exprs.ColNames()...), ","); err != nil {
		return "", nil, err
	}

	if _, err := buf.WriteString(") VALUES ("); err != nil {
		return "", nil, err
	}

	if err := session.statement.WriteArgs(buf, args); err != nil {
		return "", nil, err
	}

	if len(exprs) > 0 {
		if _, err := buf.WriteString(","); err != nil {
			return "", nil, err
		}
		if err := exprs.WriteArgs(buf); err != nil {
			return "", nil, err
		}
	}
	if _, err := buf.WriteString(");"); err != nil {
		return "", nil, err
	}

	return buf.String(), buf.Args(), nil
}

func (session *Session) genReplaceMultipleMapSQL(columns []string, argss [][]interface{}) (string, []interface{}, error) {
	var (
		buf       = builder.NewWriter()
		exprs     = session.statement.ExprColumns
		tableName = session.statement.TableName()
		quoter    = session.engine.dialect.Quoter()
	)

	if _, err := buf.WriteString(fmt.Sprintf("REPLACE INTO %s (", quoter.Quote(tableName))); err != nil {
		return "", nil, err
	}

	if err := quoter.JoinWrite(buf.Builder, append(columns, exprs.ColNames()...), ","); err != nil {
		return "", nil, err
	}

	if _, err := buf.WriteString(") VALUES "); err != nil {
		return "", nil, err
	}

	for i, args := range argss {
		if _, err := buf.WriteString("("); err != nil {
			return "", nil, err
		}
		if err := session.statement.WriteArgs(buf, args); err != nil {
			return "", nil, err
		}

		if len(exprs) > 0 {
			if _, err := buf.WriteString(","); err != nil {
				return "", nil, err
			}
			if err := exprs.WriteArgs(buf); err != nil {
				return "", nil, err
			}
		}
		if _, err := buf.WriteString(")"); err != nil {
			return "", nil, err
		}
		if i < len(argss)-1 {
			if _, err := buf.WriteString(","); err != nil {
				return "", nil, err
			}
		}
	}
	if _, err := buf.WriteString(" ;"); err != nil {
		return "", nil, err
	}

	return buf.String(), buf.Args(), nil
}
