package dialects

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"runtime"
	"strings"
	"time"

	"xorm.io/xorm/convert"
	"xorm.io/xorm/core"
	"xorm.io/xorm/schemas"
)

// from https://github.com/ydb-platform/ydb/blob/main/ydb/library/yql/sql/v1/SQLv1.g.in#L1117
var (
	ydbReservedWords = map[string]bool{
		"ABORT":             true,
		"ACTION":            true,
		"ADD":               true,
		"AFTER":             true,
		"ALL":               true,
		"ALTER":             true,
		"ANALYZE":           true,
		"AND":               true,
		"ANSI":              true,
		"ANY":               true,
		"ARRAY":             true,
		"AS":                true,
		"ASC":               true,
		"ASSUME":            true,
		"ASYNC":             true,
		"ATTACH":            true,
		"AUTOINCREMENT":     true,
		"AUTOMAP":           true,
		"BEFORE":            true,
		"BEGIN":             true,
		"BERNOULLI":         true,
		"BETWEEN":           true,
		"BITCAST":           true,
		"BY":                true,
		"CALLABLE":          true,
		"CASCADE":           true,
		"CASE":              true,
		"CAST":              true,
		"CHANGEFEED":        true,
		"CHECK":             true,
		"COLLATE":           true,
		"COLUMN":            true,
		"COLUMNS":           true,
		"COMMIT":            true,
		"COMPACT":           true,
		"CONDITIONAL":       true,
		"CONFLICT":          true,
		"CONSTRAINT":        true,
		"COVER":             true,
		"CREATE":            true,
		"CROSS":             true,
		"CUBE":              true,
		"CURRENT":           true,
		"CURRENT_TIME":      true,
		"CURRENT_DATE":      true,
		"CURRENT_TIMESTAMP": true,
		"DATABASE":          true,
		"DECIMAL":           true,
		"DECLARE":           true,
		"DEFAULT":           true,
		"DEFERRABLE":        true,
		"DEFERRED":          true,
		"DEFINE":            true,
		"DELETE":            true,
		"DESC":              true,
		"DETACH":            true,
		"DICT":              true,
		"DISABLE":           true,
		"DISCARD":           true,
		"DISTINCT":          true,
		"DO":                true,
		"DROP":              true,
		"EACH":              true,
		"ELSE":              true,
		"ERROR":             true,
		"EMPTY":             true,
		"EMPTY_ACTION":      true,
		"ENCRYPTED":         true,
		"END":               true,
		"ENUM":              true,
		"ERASE":             true,
		"ESCAPE":            true,
		"EVALUATE":          true,
		"EXCEPT":            true,
		"EXCLUDE":           true,
		"EXCLUSIVE":         true,
		"EXCLUSION":         true,
		"EXISTS":            true,
		"EXPLAIN":           true,
		"EXPORT":            true,
		"EXTERNAL":          true,
		"FAIL":              true,
		"FAMILY":            true,
		"FILTER":            true,
		"FLATTEN":           true,
		"FLOW":              true,
		"FOLLOWING":         true,
		"FOR":               true,
		"FOREIGN":           true,
		"FROM":              true,
		"FULL":              true,
		"FUNCTION":          true,
		"GLOB":              true,
		"GLOBAL":            true,
		"GROUP":             true,
		"GROUPING":          true,
		"GROUPS":            true,
		"HASH":              true,
		"HAVING":            true,
		"HOP":               true,
		"IF":                true,
		"IGNORE":            true,
		"ILIKE":             true,
		"IMMEDIATE":         true,
		"IMPORT":            true,
		"IN":                true,
		"INDEX":             true,
		"INDEXED":           true,
		"INHERITS":          true,
		"INITIALLY":         true,
		"INNER":             true,
		"INSERT":            true,
		"INSTEAD":           true,
		"INTERSECT":         true,
		"INTO":              true,
		"IS":                true,
		"ISNULL":            true,
		"JOIN":              true,
		"JSON_EXISTS":       true,
		"JSON_VALUE":        true,
		"JSON_QUERY":        true,
		"KEY":               true,
		"LEFT":              true,
		"LIKE":              true,
		"LIMIT":             true,
		"LIST":              true,
		"LOCAL":             true,
		"MATCH":             true,
		"NATURAL":           true,
		"NO":                true,
		"NOT":               true,
		"NOTNULL":           true,
		"NULL":              true,
		"NULLS":             true,
		"OBJECT":            true,
		"OF":                true,
		"OFFSET":            true,
		"ON":                true,
		"ONLY":              true,
		"OPTIONAL":          true,
		"OR":                true,
		"ORDER":             true,
		"OTHERS":            true,
		"OUTER":             true,
		"OVER":              true,
		"PARTITION":         true,
		"PASSING":           true,
		"PASSWORD":          true,
		"PLAN":              true,
		"PRAGMA":            true,
		"PRECEDING":         true,
		"PRESORT":           true,
		"PRIMARY":           true,
		"PROCESS":           true,
		"RAISE":             true,
		"RANGE":             true,
		"REDUCE":            true,
		"REFERENCES":        true,
		"REGEXP":            true,
		"REINDEX":           true,
		"RELEASE":           true,
		"RENAME":            true,
		"REPEATABLE":        true,
		"REPLACE":           true,
		"RESET":             true,
		"RESOURCE":          true,
		"RESPECT":           true,
		"RESTRICT":          true,
		"RESULT":            true,
		"RETURN":            true,
		"RETURNING":         true,
		"REVERT":            true,
		"RIGHT":             true,
		"RLIKE":             true,
		"ROLLBACK":          true,
		"ROLLUP":            true,
		"ROW":               true,
		"ROWS":              true,
		"SAMPLE":            true,
		"SAVEPOINT":         true,
		"SCHEMA":            true,
		"SELECT":            true,
		"SEMI":              true,
		"SET":               true,
		"SETS":              true,
		"STREAM":            true,
		"STRUCT":            true,
		"SUBQUERY":          true,
		"SYMBOLS":           true,
		"SYNC":              true,
		"SYSTEM":            true,
		"TABLE":             true,
		"TABLESAMPLE":       true,
		"TABLESTORE":        true,
		"TAGGED":            true,
		"TEMP":              true,
		"TEMPORARY":         true,
		"THEN":              true,
		"TIES":              true,
		"TO":                true,
		"TRANSACTION":       true,
		"TRIGGER":           true,
		"TUPLE":             true,
		"UNBOUNDED":         true,
		"UNCONDITIONAL":     true,
		"UNION":             true,
		"UNIQUE":            true,
		"UNKNOWN":           true,
		"UPDATE":            true,
		"UPSERT":            true,
		"USE":               true,
		"USER":              true,
		"USING":             true,
		"VACUUM":            true,
		"VALUES":            true,
		"VARIANT":           true,
		"VIEW":              true,
		"VIRTUAL":           true,
		"WHEN":              true,
		"WHERE":             true,
		"WINDOW":            true,
		"WITH":              true,
		"WITHOUT":           true,
		"WRAPPER":           true,
		"XOR":               true,
		"TRUE":              true,
		"FALSE":             true,
	}

	ydbQuoter = schemas.Quoter{
		Prefix:     '`',
		Suffix:     '`',
		IsReserved: schemas.AlwaysReserve,
	}

	ydbSDK = string("")
	ydbDSN = string("")
)

type ydb struct {
	Base
	ydb *sql.DB
}

type QueryMode int

const (
	UnknownMetadataQueryMode = QueryMode(iota)

	VersionQueryMode
	IsTableExistQueryMode
	IsColumnExistQueryMode
	GetColumnsQueryMode
	GetTablesQueryMode
	GetIndexesQueryMode
)

func (db *ydb) autoPrefix(s string) string {
	dbName := db.dialect.URI().DBName
	if !strings.HasPrefix(s, dbName) {
		return path.Join(dbName, s)
	}
	return s
}

func removeOptional(s string) string {
	if strings.HasPrefix(s, "Optional") {
		s = strings.TrimPrefix(s, "Optional<")
		s = strings.TrimSuffix(s, ">")
	}
	return s
}

func (db *ydb) Init(uri *URI) error {
	db.quoter = ydbQuoter

	var err error
	db.ydb, err = sql.Open(ydbSDK, ydbDSN)
	if err != nil {
		return err
	}
	db.ydb.SetMaxOpenConns(50)
	db.ydb.SetMaxIdleConns(50)
	db.ydb.SetConnMaxIdleTime(time.Second)

	runtime.SetFinalizer(db.ydb, func(ydb *sql.DB) {
		_ = ydb.Close()
	})

	return db.Base.Init(db, uri)
}

type ydbRows struct {
	rowsi    driver.Rows
	lastCols []driver.Value
	lastErr  error
	closed   bool
}

func assignValue(dst, src interface{}) error {
	switch s := src.(type) {
	case string:
		switch d := dst.(type) {
		case *string:
			if d == nil {
				return fmt.Errorf("can not assign to nil pointer")
			}
			*d = s
			return nil
		}
	case bool:
		switch d := dst.(type) {
		case *bool:
			if d == nil {
				return fmt.Errorf("can not assign to nil pointer")
			}
			*d = s
			return nil
		}
	}
	return fmt.Errorf("not support type %T", src)
}

func (rs *ydbRows) Scan(dest ...interface{}) error {
	if len(dest) != len(rs.lastCols) {
		return fmt.Errorf("len(dest) is not equal len(rs.lastCols)")
	}
	for i, dst := range dest {
		err := assignValue(dst, rs.lastCols[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *ydbRows) Close() error {
	rs.closed = true
	return rs.rowsi.Close()
}

func (rs *ydbRows) Next() bool {
	if rs.closed {
		return false
	}

	if rs.lastCols == nil {
		rs.lastCols = make([]driver.Value, len(rs.rowsi.Columns()))
	}

	rs.lastErr = rs.rowsi.Next(rs.lastCols)
	if rs.lastErr != nil {
		if rs.lastErr != io.EOF {
			rs.Close()
			return false
		}
		return false
	}
	return true
}

func (rs *ydbRows) Err() error {
	if rs.lastErr != nil && rs.lastErr != io.EOF {
		return rs.lastErr
	}
	return nil
}

func (db *ydb) Raw(ctx context.Context, query string, option QueryMode, args ...interface{}) (*ydbRows, error) {
	conn, err := db.ydb.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	dargs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		dargs[i].Name = arg.(sql.NamedArg).Name
		dargs[i].Value = arg.(sql.NamedArg).Value
		dargs[i].Ordinal = i + 1
	}

	var rowsi driver.Rows
	err = conn.Raw(func(driverConn interface{}) (err error) {
		c, ok := driverConn.(interface {
			Version(context.Context) (driver.Rows, error)
			IsTableExists(context.Context, string, []driver.NamedValue) (driver.Rows, error)
			IsColumnExists(context.Context, string, []driver.NamedValue) (driver.Rows, error)
			GetColumns(context.Context, string, []driver.NamedValue) (driver.Rows, error)
			GetTables(context.Context, string, []driver.NamedValue) (driver.Rows, error)
			GetIndexes(context.Context, string, []driver.NamedValue) (driver.Rows, error)
		})
		if !ok {
			return fmt.Errorf("driver does not support query metadata")
		}
		switch option {
		case VersionQueryMode:
			rowsi, err = c.Version(ctx)
		case IsTableExistQueryMode:
			rowsi, err = c.IsTableExists(ctx, query, dargs)
		case IsColumnExistQueryMode:
			rowsi, err = c.IsColumnExists(ctx, query, dargs)
		case GetColumnsQueryMode:
			rowsi, err = c.GetColumns(ctx, query, dargs)
		case GetTablesQueryMode:
			rowsi, err = c.GetTables(ctx, query, dargs)
		case GetIndexesQueryMode:
			rowsi, err = c.GetIndexes(ctx, query, dargs)
		}
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	rows := &ydbRows{
		rowsi: rowsi,
	}

	return rows, nil
}

func (db *ydb) Version(ctx context.Context, queryer core.Queryer) (*schemas.Version, error) {
	rows, err := db.Raw(ctx, "SELECT $Version", VersionQueryMode)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var version string
	if !rows.Next() {
		return nil, errors.New("unknow version")
	}

	if err = rows.Scan(&version); err != nil {
		return nil, err
	}

	if strings.HasPrefix(version, "YDB Server") {
		return &schemas.Version{
			Number:  strings.TrimPrefix(version, "YDB Server v"),
			Edition: "YDB Server",
		}, nil
	}
	return nil, errors.New("unknow version")
}

func (db *ydb) Features() *DialectFeatures {
	return &DialectFeatures{
		AutoincrMode: -1,
	}
}

func (db *ydb) AutoIncrStr() string {
	return ""
}

func (db *ydb) IsReserved(name string) bool {
	_, ok := ydbReservedWords[strings.ToUpper(name)]
	return ok
}

// always quote
func (db *ydb) SetQuotePolicy(quotePolicy QuotePolicy) {}

var (
	// numeric types
	yql_Bool = "Bool"

	yql_Int8  = "Int8"
	yql_Int16 = "Int16"
	yql_Int32 = "Int32"
	yql_Int64 = "Int64"

	yql_Uint8  = "Uint8"
	yql_Uint16 = "Uint16"
	yql_Uint32 = "Uint32"
	yql_Uint64 = "Uint64"

	yql_Float   = "Float"
	yql_Double  = "Double"
	yql_Decimal = "Decimal"

	// string types
	yql_String       = "String"
	yql_Utf8         = "Utf8"
	yql_Json         = "Json"
	yql_JsonDocument = "JsonDocument"
	yql_Yson         = "Yson"

	// Data and Time
	yql_Date      = "Date"
	yql_DateTime  = "DateTime"
	yql_Timestamp = "Timestamp"
	yql_Interval  = "Interval"

	// Containers
	yql_List = "List"
)

func toYQLDataType(t string, defaultLength, defaultLength2 int64) (yqlType string) {
	switch v := t; v {
	case schemas.Bool:
		yqlType = yql_Bool
		return
	case schemas.TinyInt:
		yqlType = yql_Int8
		return
	case schemas.UnsignedTinyInt:
		yqlType = yql_Uint8
		return
	case schemas.SmallInt:
		yqlType = yql_Int16
		return
	case schemas.UnsignedSmallInt:
		yqlType = yql_Uint16
		return
	case schemas.MediumInt:
		yqlType = yql_Int32
		return
	case schemas.UnsignedMediumInt:
		yqlType = yql_Uint32
		return
	case schemas.BigInt:
		yqlType = yql_Int64
		return
	case schemas.UnsignedBigInt:
		yqlType = yql_Uint64
		return
	case schemas.Float:
		yqlType = yql_Float
		return
	case schemas.Double:
		yqlType = yql_Double
		return
	case schemas.Blob:
		yqlType = yql_String
		return
	case schemas.Json:
		yqlType = yql_Json
		return
	case schemas.Array:
		yqlType = yql_List
		return
	case schemas.Varchar:
		yqlType = yql_Utf8
		return
	case schemas.TimeStamp:
		yqlType = yql_Timestamp
		return
	case schemas.Interval:
		yqlType = yql_Interval
		return
	default:
		yqlType = yql_String
	}
	return
}

func (db *ydb) SQLType(column *schemas.Column) string {
	return toYQLDataType(column.SQLType.Name, column.SQLType.DefaultLength, column.SQLType.DefaultLength2)
}

func yqlToSQLType(yqlType string) (sqlType schemas.SQLType) {
	switch yqlType {
	case yql_Bool:
		sqlType = schemas.SQLType{schemas.Bool, 0, 0}
		return
	case yql_Int8:
		sqlType = schemas.SQLType{schemas.TinyInt, 0, 0}
		return
	case yql_Uint8:
		sqlType = schemas.SQLType{schemas.UnsignedTinyInt, 0, 0}
		return
	case yql_Int16:
		sqlType = schemas.SQLType{schemas.SmallInt, 0, 0}
		return
	case yql_Uint16:
		sqlType = schemas.SQLType{schemas.UnsignedSmallInt, 0, 0}
		return
	case yql_Int32:
		sqlType = schemas.SQLType{schemas.MediumInt, 0, 0}
		return
	case yql_Uint32:
		sqlType = schemas.SQLType{schemas.UnsignedMediumInt, 0, 0}
		return
	case yql_Int64:
		sqlType = schemas.SQLType{schemas.BigInt, 0, 0}
		return
	case yql_Uint64:
		sqlType = schemas.SQLType{schemas.UnsignedBigInt, 0, 0}
		return
	case yql_Float:
		sqlType = schemas.SQLType{schemas.Float, 0, 0}
		return
	case yql_Double:
		sqlType = schemas.SQLType{schemas.Double, 0, 0}
		return
	case yql_String:
		sqlType = schemas.SQLType{schemas.Blob, 0, 0}
		return
	case yql_Json:
		sqlType = schemas.SQLType{schemas.Json, 0, 0}
		return
	case yql_List:
		sqlType = schemas.SQLType{schemas.Array, 0, 0}
		return
	case yql_Utf8:
		sqlType = schemas.SQLType{schemas.Varchar, 255, 0}
		return
	case yql_Timestamp:
		sqlType = schemas.SQLType{schemas.TimeStamp, 0, 0}
		return
	case yql_Interval:
		sqlType = schemas.SQLType{schemas.Interval, 0, 0}
		return
	default:
		sqlType = schemas.SQLType{schemas.Text, 0, 0}
	}
	return
}

// https://pkg.go.dev/database/sql#ColumnType.DatabaseTypeName
func (db *ydb) ColumnTypeKind(t string) int {
	switch t {
	case "BOOL":
		return schemas.BOOL_TYPE
	case "INT8", "INT16", "INT32", "INT64", "UINT8", "UINT16", "UINT32", "UINT64":
		return schemas.NUMERIC_TYPE
	case "UTF8":
		return schemas.TEXT_TYPE
	case "TIMESTAMP":
		return schemas.TIME_TYPE
	default:
		return schemas.UNKNOW_TYPE
	}
}

func (db *ydb) IndexCheckSQL(tableName, indexName string) (string, []interface{}) {
	return "", nil
}

func (db *ydb) IsTableExist(
	queryer core.Queryer,
	ctx context.Context,
	tableName string) (bool, error) {
	sys := ".sys/ydb_info"
	pathToTable := db.autoPrefix(tableName)
	query := fmt.Sprintf("SELECT TableName FROM `%s` WHERE TableName = $TableName", sys)

	rows, err := db.Raw(ctx, query, IsTableExistQueryMode, sql.Named("TableName", pathToTable))

	if err != nil {
		return false, err
	}
	defer rows.Close()

	if rows.Next() {
		return true, nil
	}
	return false, nil
}

func (db *ydb) AddColumnSQL(tableName string, column *schemas.Column) string {
	quote := db.dialect.Quoter()

	pathToTable := quote.Quote(db.autoPrefix(tableName))
	columnName := quote.Quote(column.Name)
	dataType := db.SQLType(column)

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", pathToTable, columnName, dataType))

	return buf.String()
}

// YDB does not support this operation
func (db *ydb) ModifyColumnSQL(tableName string, column *schemas.Column) string {
	return ""
}

// SYNC by default
func (db *ydb) CreateIndexSQL(tableName string, index *schemas.Index) string {
	quote := db.dialect.Quoter()

	pathToTable := quote.Quote(db.autoPrefix(tableName))
	indexName := quote.Quote(index.Name)

	colsIndex := make([]string, len(index.Cols))
	for i := 0; i < len(index.Cols); i++ {
		colsIndex[i] = quote.Quote(index.Cols[i])
	}

	indexOn := strings.Join(colsIndex, ",")

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("ALTER TABLE %s ADD INDEX %s GLOBAL ON ( %s );", pathToTable, indexName, indexOn))

	return buf.String()
}

func (db *ydb) DropIndexSQL(tableName string, index *schemas.Index) string {
	quote := db.dialect.Quoter()

	pathToTable := quote.Quote(db.autoPrefix(tableName))
	indexName := quote.Quote(index.Name)

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("ALTER TABLE %s DROP INDEX %s;", pathToTable, indexName))

	return buf.String()
}

func (db *ydb) IsColumnExist(
	queryer core.Queryer,
	ctx context.Context,
	tableName,
	columnName string) (bool, error) {
	sys := ".sys/ydb_info"
	pathToTable := db.autoPrefix(tableName)

	query := fmt.Sprintf("SELECT ColumnName FROM `%s` WHERE TableName = $TableName AND ColumnName = $ColumnName", sys)

	rows, err := db.Raw(ctx, query, IsColumnExistQueryMode,
		sql.Named("TableName", pathToTable),
		sql.Named("ColumnName", columnName))

	if err != nil {
		return false, err
	}
	defer rows.Close()

	if rows.Next() {
		return true, nil
	}
	return false, nil
}

func (db *ydb) GetColumns(queryer core.Queryer, ctx context.Context, tableName string) (
	[]string,
	map[string]*schemas.Column,
	error) {
	sys := ".sys/ydb_info"
	pathToTable := db.autoPrefix(tableName)

	query := fmt.Sprintf("SELECT ColumnName, TableName, DataType, IsPrimaryKey FROM `%s` WHERE TableName = $TableName", sys)

	rows, err := db.Raw(ctx, query, GetColumnsQueryMode, sql.Named("TableName", pathToTable))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	colNames := make([]string, 0)
	colMaps := make(map[string]*schemas.Column)
	for rows.Next() {
		var columnName, dataType string
		var isPk bool
		if err := rows.Scan(&columnName, &tableName, &dataType, &isPk); err != nil {
			return nil, nil, err
		}

		dataType = removeOptional(dataType)
		col := &schemas.Column{
			Name:         columnName,
			TableName:    tableName,
			SQLType:      yqlToSQLType(dataType),
			IsPrimaryKey: isPk,
			Nullable:     !isPk,
			Indexes:      make(map[string]int),
		}

		colNames = append(colNames, columnName)
		colMaps[columnName] = col
	}

	if rows.Err() != nil {
		return nil, nil, rows.Err()
	}

	return colNames, colMaps, nil
}

func (db *ydb) GetTables(queryer core.Queryer, ctx context.Context) ([]*schemas.Table, error) {
	sys := ".sys/ydb_info"
	dbName := db.URI().DBName
	query := fmt.Sprintf("SELECT TableName FROM `%s` WHERE DatabaseName = $DatabaseName", sys)

	rows, err := db.
		Raw(
			ctx,
			query,
			GetTablesQueryMode,
			sql.Named("DatabaseName", dbName),
			sql.Named("IgnoreDirs", []string{".sys", ".sys_health"}),
		)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]*schemas.Table, 0)
	for rows.Next() {
		table := schemas.NewEmptyTable()
		if err := rows.Scan(&table.Name); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return tables, nil
}

func (db *ydb) GetIndexes(
	queryer core.Queryer,
	ctx context.Context,
	tableName string) (map[string]*schemas.Index, error) {
	sys := ".sys/ydb_info"
	pathToTable := db.autoPrefix(tableName)
	query := fmt.Sprintf("SELECT IndexName, Columns FROM `%s` WHERE TableName = $TableName", sys)

	rows, err := db.Raw(ctx, query, GetIndexesQueryMode, sql.Named("TableName", pathToTable))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexes := make(map[string]*schemas.Index, 0)
	for rows.Next() {
		var indexName string
		var columns string
		if err := rows.Scan(&indexName, &columns); err != nil {
			return nil, err
		}
		cols := strings.Split(columns, ",")
		indexes[indexName] = &schemas.Index{
			Name: indexName,
			Type: schemas.IndexType,
			Cols: cols,
		}
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return indexes, nil
}

func (db *ydb) CreateTableSQL(
	ctx context.Context,
	queryer core.Queryer,
	table *schemas.Table,
	tableName string) (string, bool, error) {

	quote := db.dialect.Quoter()

	pathToTable := quote.Quote(db.autoPrefix(tableName))

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("CREATE TABLE %s ( ", pathToTable))

	// 	build primary key
	if len(table.PrimaryKeys) == 0 {
		return "", false, errors.New("table must have at least one primary key")
	}
	pk := make([]string, len(table.PrimaryKeys))
	pkMap := make(map[string]bool)
	for i := 0; i < len(table.PrimaryKeys); i++ {
		pk[i] = quote.Quote(table.PrimaryKeys[i])
		pkMap[pk[i]] = true
	}
	primaryKey := fmt.Sprintf("PRIMARY KEY ( %s )", strings.Join(pk, ", "))

	// build column
	columnsList := []string{}
	for _, c := range table.Columns() {
		columnName := quote.Quote(c.Name)
		dataType := db.SQLType(c)

		if _, isPk := pkMap[columnName]; isPk {
			columnsList = append(columnsList, fmt.Sprintf("%s %s NOT NULL", columnName, dataType))
		} else {
			columnsList = append(columnsList, fmt.Sprintf("%s %s", columnName, dataType))
		}
	}
	joinColumns := strings.Join(columnsList, ", ")

	// build index
	indexList := []string{}
	for indexName, index := range table.Indexes {
		name := quote.Quote(indexName)
		onCols := make([]string, len(index.Cols))
		for i := 0; i < len(index.Cols); i++ {
			onCols[i] = quote.Quote(index.Cols[i])
		}
		indexList = append(indexList,
			fmt.Sprintf(
				"INDEX %s GLOBAL ON ( %s )",
				name, strings.Join(onCols, ", ")))
	}
	joinIndexes := strings.Join(indexList, ", ")

	if joinIndexes != "" {
		buf.WriteString(strings.Join([]string{joinColumns, joinIndexes, primaryKey}, ", "))
	} else {
		buf.WriteString(strings.Join([]string{joinColumns, primaryKey}, ", "))
	}

	buf.WriteString(" );")

	return buf.String(), true, nil
}

func (db *ydb) DropTableSQL(tableName string) (string, bool) {
	quote := db.dialect.Quoter()

	pathToTable := quote.Quote(db.autoPrefix(tableName))

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("DROP TABLE %s;", pathToTable))

	return buf.String(), false
}

// https://github.com/ydb-platform/ydb-go-sdk/blob/master/SQL.md#specifying-query-parameters-
func (db *ydb) Filters() []Filter {
	return []Filter{&SeqFilter{
		Prefix: "$",
		Start:  1,
	}}
}

type ydbDriver struct {
	baseDriver
}

func (ydbDrv *ydbDriver) Features() *DriverFeatures {
	return &DriverFeatures{
		SupportReturnInsertedID: false,
	}
}

// DSN format: https://github.com/ydb-platform/ydb-go-sdk/blob/a804c31be0d3c44dfd7b21ed49d863619217b11d/connection.go#L339
func (ydbDrv *ydbDriver) Parse(driverName, dataSourceName string) (*URI, error) {
	info := &URI{DBType: schemas.YDB}
	ydbDSN = dataSourceName
	ydbSDK = driverName

	uri, err := url.Parse(dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed on parse data source %v", dataSourceName)
	}

	const (
		secure   = "grpcs"
		insecure = "grpc"
	)

	if uri.Scheme != secure && uri.Scheme != insecure {
		return nil, fmt.Errorf("unsupported scheme %v", uri.Scheme)
	}
	// info.Schema = uri.Scheme

	info.Host = uri.Host
	if spl := strings.Split(uri.Host, ":"); len(spl) > 1 {
		info.Host = spl[0]
		info.Port = spl[1]
	}

	info.DBName = uri.Path
	if info.DBName == "" {
		return nil, errors.New("database path can not be empty")
	}

	if uri.User != nil {
		info.Passwd, _ = uri.User.Password()
		info.User = uri.User.Username()
	}

	return info, nil
}

// ydb-go-sdk does not support ColumnType.
// https://pkg.go.dev/database/sql#ColumnType.DatabaseTypeName
func (ydbDrv *ydbDriver) GenScanResult(columnType string) (interface{}, error) {
	columnType = strings.ToUpper(removeOptional(columnType))
	switch columnType {
	case "BOOL":
		var ret sql.NullBool
		return &ret, nil
	case "INT8":
		var ret convert.NullInt8
		return &ret, nil
	case "INT16":
		var ret sql.NullInt16
		return &ret, nil
	case "INT32":
		var ret sql.NullInt32
		return &ret, nil
	case "INT64":
		var ret sql.NullInt64
		return &ret, nil
	case "UINT8":
		var ret sql.NullByte
		return &ret, nil
	case "UINT16":
		var ret convert.NullUint16
		return &ret, nil
	case "UINT32":
		var ret convert.NullUint32
		return &ret, nil
	case "UINT64":
		var ret convert.NullUint64
		return &ret, nil
	case "FLOAT":
		var ret convert.NullFloat32
		return &ret, nil
	case "DOUBLE":
		var ret sql.NullFloat64
		return &ret, nil
	case "UTF8":
		var ret sql.NullString
		return &ret, nil
	case "TIMESTAMP":
		var ret sql.NullTime
		return &ret, nil
	case "INTERVAL":
		var ret convert.NullDuration
		return &ret, nil
	default:
		var ret sql.RawBytes
		return &ret, nil
	}
}
