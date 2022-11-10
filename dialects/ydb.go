package dialects

import (
	"context"
	"database/sql"
	"strings"

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
)

type ydb struct {
	Base
}

func (db *ydb) Init(uri *URI) error {
	db.quoter = ydbQuoter
	return db.Base.Init(db, uri)
}

func (db *ydb) Version(ctx context.Context, queryer core.Queryer) (*schemas.Version, error) {
	return nil, nil
}

func (db *ydb) Features() *DialectFeatures {
	return &DialectFeatures{
		// AutoincrMode: SequenceAutoincrMode,
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
	ydb_Bool = "Bool"

	ydb_Int32 = "Int32"
	ydb_Int64 = "Int64"

	ydb_Uint8  = "Uint8"
	ydb_Uint32 = "Uint32"
	ydb_Uint64 = "Uint64"

	ydb_Float   = "Float"
	ydb_Double  = "Double"
	ydb_Decimal = "Decimal"

	// string types
	ydb_String       = "String"
	ydb_Utf8         = "Utf8"
	ydb_Json         = "Json"
	ydb_JsonDocument = "JsonDocument"
	ydb_Yson         = "Yson"

	// Data and Time
	ydb_Date      = "Date"
	ydb_DateTime  = "DateTime"
	ydb_Timestamp = "Timestamp"
	ydb_Interval  = "Interval"
)

func (db *ydb) SQLType(column *schemas.Column) string {
	var res string
	switch v := column.SQLType.Name; v {
	case schemas.Int, schemas.Integer, schemas.TinyInt, schemas.SmallInt, schemas.MediumInt:
		res = ydb_Int32
	case schemas.UnsignedInt:
		res = ydb_Uint32
	case schemas.BigInt:
		res = ydb_Int64
	case schemas.UnsignedBigInt:
		res = ydb_Uint64
	case schemas.Float:
		res = ydb_Float
	case schemas.Double:
		res = ydb_Double
	case schemas.Bool:
		res = ydb_Bool
	case schemas.Char, schemas.TinyText, schemas.Text, schemas.MediumText, schemas.LongText:
		res = ydb_Utf8
	case schemas.Varchar:
		if column.SQLType.DefaultLength == 255 {
			res = ydb_Utf8
		} else {
			res = ydb_String
		}
	case schemas.Date:
		res = ydb_Date
	case schemas.TimeStamp:
		res = ydb_Timestamp
	case schemas.DateTime:
		res = ydb_DateTime
	case schemas.Decimal:
		res = ydb_Decimal
	default:
		res = ydb_String
	}

	return res
}

// ydb-go-sdk does not support ColumnType.
// https://pkg.go.dev/database/sql#ColumnType.DatabaseTypeName
func (db *ydb) ColumnTypeKind(t string) int {
	return schemas.UNKNOW_TYPE
}

func (db *ydb) IndexCheckSQL(tableName, indexName string) (string, []interface{}) {
	// TODO
	return "", nil
}

func (db *ydb) IsTableExist(
	queryer core.Queryer,
	ctx context.Context,
	tableName string) (bool, error) {
	// TODO
	return true, nil
}

func (db *ydb) AddColumnSQL(tableName string, column *schemas.Column) string {
	// TODO
	return ""
}

func (db *ydb) ModifyColumnSQL(tableName string, column *schemas.Column) string {
	// TODO
	return ""
}

func (db *ydb) DropIndexSQL(tableName string, index *schemas.Index) string {
	// TODO
	return ""
}

func (db *ydb) IsColumnExist(
	queryer core.Queryer,
	ctx context.Context,
	tableName,
	columnName string) (bool, error) {
	// TODO
	return true, nil
}

func (db *ydb) GetColumns(queryer core.Queryer, ctx context.Context, tableName string) (
	[]string,
	map[string]*schemas.Column,
	error) {
	// TODO
	return nil, nil, nil
}

func (db *ydb) GetTables(queryer core.Queryer, ctx context.Context) ([]*schemas.Table, error) {
	return nil, nil
}

func (db *ydb) GetIndexes(
	queryer core.Queryer,
	ctx context.Context,
	tableName string) (map[string]*schemas.Index, error) {
	// TODO
	return nil, nil
}

func (db *ydb) CreateTableSQL(
	ctx context.Context,
	queryer core.Queryer,
	table *schemas.Table,
	tableName string) (string, bool, error) {
	// TODO
	return "", true, nil
}

// ydb already use $ for query parameters
func (db *ydb) Filters() []Filter {
	return []Filter{&FakeFilter{}}
}

type ydbDriver struct {
	baseDriver
}

func (ydbDrv *ydbDriver) Features() *DriverFeatures {
	return &DriverFeatures{
		SupportReturnInsertedID: false,
	}
}

func (ydbDrv *ydbDriver) Parse(driverName, dataSourceName string) (*URI, error) {
	// TODO
	return nil, nil
}

// ydb-go-sdk does not support ColumnType.
// https://pkg.go.dev/database/sql#ColumnType.DatabaseTypeName
func (ydbDrv *ydbDriver) GenScanResult(columnType string) (interface{}, error) {
	var ret sql.RawBytes
	return &ret, nil
}
