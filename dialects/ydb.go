package dialects

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
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
)

const (
	// numeric types
	yql_Bool = "BOOL"

	yql_Int8  = "INT8"
	yql_Int16 = "INT16"
	yql_Int32 = "INT32"
	yql_Int64 = "INT64"

	yql_Uint8  = "UINT8"
	yql_Uint16 = "UINT16"
	yql_Uint32 = "UINT32"
	yql_Uint64 = "UINT64"

	yql_Float   = "FLOAT"
	yql_Double  = "DOUBLE"
	yql_Decimal = "DECIMAL"

	// string types
	yql_String       = "STRING"
	yql_Utf8         = "UTF8"
	yql_Json         = "JSON"
	yql_JsonDocument = "JSONDOCUMENT"
	yql_Yson         = "YSON"

	// Data and Time
	yql_Date      = "DATE"
	yql_DateTime  = "DATETIME"
	yql_Timestamp = "TIMESTAMP"
	yql_Interval  = "INTERVAL"

	// Containers
	yql_List = "LIST"
)

func toYQLDataType(t string, defaultLength, defaultLength2 int64) string {
	switch v := t; v {
	case schemas.Bool, schemas.Boolean:
		return yql_Bool
	case schemas.TinyInt:
		return yql_Int8
	case schemas.UnsignedTinyInt:
		return yql_Uint8
	case schemas.SmallInt:
		return yql_Int16
	case schemas.UnsignedSmallInt:
		return yql_Uint16
	case schemas.MediumInt:
		return yql_Int32
	case schemas.UnsignedMediumInt:
		return yql_Uint32
	case schemas.BigInt:
		return yql_Int64
	case schemas.UnsignedBigInt:
		return yql_Uint64
	case schemas.Int, schemas.Integer:
		return yql_Int32
	case schemas.UnsignedInt:
		return yql_Uint32
	case schemas.Float:
		return yql_Float
	case schemas.Double:
		return yql_Double
	case schemas.Blob:
		return yql_String
	case schemas.Json:
		return yql_Json
	case schemas.Array:
		return yql_List
	case schemas.Varchar, schemas.Text:
		return yql_Utf8
	case schemas.TimeStamp, schemas.DateTime:
		return yql_Timestamp
	case schemas.Interval:
		return yql_Interval
	default:
		return yql_String
	}
}

func yqlToSQLType(yqlType string) schemas.SQLType {
	switch yqlType {
	case yql_Bool:
		return schemas.SQLType{Name: schemas.Bool, DefaultLength: 0, DefaultLength2: 0}
	case yql_Int8:
		return schemas.SQLType{Name: schemas.TinyInt, DefaultLength: 0, DefaultLength2: 0}
	case yql_Uint8:
		return schemas.SQLType{Name: schemas.UnsignedTinyInt, DefaultLength: 0, DefaultLength2: 0}
	case yql_Int16:
		return schemas.SQLType{Name: schemas.SmallInt, DefaultLength: 0, DefaultLength2: 0}
	case yql_Uint16:
		return schemas.SQLType{Name: schemas.UnsignedSmallInt, DefaultLength: 0, DefaultLength2: 0}
	case yql_Int32:
		return schemas.SQLType{Name: schemas.MediumInt, DefaultLength: 0, DefaultLength2: 0}
	case yql_Uint32:
		return schemas.SQLType{Name: schemas.UnsignedMediumInt, DefaultLength: 0, DefaultLength2: 0}
	case yql_Int64:
		return schemas.SQLType{Name: schemas.BigInt, DefaultLength: 0, DefaultLength2: 0}
	case yql_Uint64:
		return schemas.SQLType{Name: schemas.UnsignedBigInt, DefaultLength: 0, DefaultLength2: 0}
	case yql_Float:
		return schemas.SQLType{Name: schemas.Float, DefaultLength: 0, DefaultLength2: 0}
	case yql_Double:
		return schemas.SQLType{Name: schemas.Double, DefaultLength: 0, DefaultLength2: 0}
	case yql_String:
		return schemas.SQLType{Name: schemas.Blob, DefaultLength: 0, DefaultLength2: 0}
	case yql_Json:
		return schemas.SQLType{Name: schemas.Json, DefaultLength: 0, DefaultLength2: 0}
	case yql_List:
		return schemas.SQLType{Name: schemas.Array, DefaultLength: 0, DefaultLength2: 0}
	case yql_Utf8:
		return schemas.SQLType{Name: schemas.Varchar, DefaultLength: 255, DefaultLength2: 0}
	case yql_Timestamp:
		return schemas.SQLType{Name: schemas.TimeStamp, DefaultLength: 0, DefaultLength2: 0}
	case yql_Interval:
		return schemas.SQLType{Name: schemas.Interval, DefaultLength: 0, DefaultLength2: 0}
	default:
		return schemas.SQLType{Name: schemas.Blob, DefaultLength: 0, DefaultLength2: 0}
	}
}

func removeOptional(s string) string {
	if s = strings.ToUpper(s); strings.HasPrefix(s, "OPTIONAL") {
		s = strings.TrimPrefix(s, "OPTIONAL<")
		s = strings.TrimSuffix(s, ">")
	}
	return s
}

type ydb struct {
	Base

	tableParams map[string]string
}

func (db *ydb) Init(uri *URI) error {
	db.quoter = ydbQuoter
	return db.Base.Init(db, uri)
}

func (db *ydb) getDB(queryer interface{}) *core.DB {
	if internalDB, ok := queryer.(*core.DB); ok {
		return internalDB
	}
	if txGetDB, ok := queryer.(interface {
		GetDB() *core.DB
	}); ok {
		return txGetDB.GetDB()
	}
	return nil
}

func (db *ydb) WithConn(queryer core.Queryer, ctx context.Context, f func(context.Context, *sql.Conn) error) error {
	coreDB := db.getDB(queryer)
	if coreDB == nil {
		return fmt.Errorf("`*core.DB` not found")
	}

	cc, err := coreDB.Conn(ctx)
	if err != nil {
		return err
	}
	defer cc.Close()

	err = f(ctx, cc)

	return err
}

func (db *ydb) WithConnRaw(queryer core.Queryer, ctx context.Context, f func(d interface{}) error) (err error) {
	err = db.WithConn(queryer, ctx, func(ctx context.Context, cc *sql.Conn) error {
		err = cc.Raw(f)
		return err
	})
	return err
}

func (db *ydb) SetParams(tableParams map[string]string) {
	db.tableParams = tableParams
}

func (db *ydb) Features() *DialectFeatures {
	return &DialectFeatures{
		AutoincrMode: -1,
	}
}

// unsupported feature
func (db *ydb) IsSequenceExist(_ context.Context, _ core.Queryer, _ string) (bool, error) {
	return false, nil
}

func (db *ydb) AutoIncrStr() string {
	return ""
}

func (db *ydb) IsReserved(name string) bool {
	_, ok := ydbReservedWords[strings.ToUpper(name)]
	return ok
}

func (db *ydb) SetQuotePolicy(quotePolicy QuotePolicy) {
	switch quotePolicy {
	case QuotePolicyNone:
		q := ydbQuoter
		q.IsReserved = schemas.AlwaysNoReserve
		db.quoter = q
	case QuotePolicyReserved:
		q := ydbQuoter
		q.IsReserved = db.IsReserved
		db.quoter = q
	case QuotePolicyAlways:
		fallthrough
	default:
		db.quoter = ydbQuoter
	}
}

func (db *ydb) SQLType(column *schemas.Column) string {
	return toYQLDataType(column.SQLType.Name, column.SQLType.DefaultLength, column.SQLType.DefaultLength2)
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

func (db *ydb) Version(ctx context.Context, queryer core.Queryer) (_ *schemas.Version, err error) {
	var version string
	err = db.WithConnRaw(queryer, ctx, func(dc interface{}) error {
		q, ok := dc.(interface {
			Version(ctx context.Context) (string, error)
		})
		if !ok {
			return fmt.Errorf("driver does not support query metadata")
		}
		version, err = q.Version(ctx)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &schemas.Version{
		Edition: version,
	}, nil
}

func (db *ydb) IndexCheckSQL(tableName, indexName string) (string, []interface{}) {
	return "", nil
}

func (db *ydb) IsTableExist(
	queryer core.Queryer,
	ctx context.Context,
	tableName string) (_ bool, err error) {
	var exists bool
	err = db.WithConnRaw(queryer, ctx, func(dc interface{}) error {
		q, ok := dc.(interface {
			IsTableExists(context.Context, string) (bool, error)
		})
		if !ok {
			return fmt.Errorf("driver does not support query metadata")
		}
		exists, err = q.IsTableExists(ctx, tableName)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return false, err
	}
	return exists, nil
}

func (db *ydb) AddColumnSQL(tableName string, col *schemas.Column) string {
	quote := db.dialect.Quoter()
	tableName = quote.Quote(tableName)
	columnName := quote.Quote(col.Name)
	dataType := db.SQLType(col)

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", tableName, columnName, dataType))

	return buf.String()
}

// YDB does not support this operation
func (db *ydb) ModifyColumnSQL(tableName string, column *schemas.Column) string {
	return ""
}

// SYNC by default
func (db *ydb) CreateIndexSQL(tableName string, index *schemas.Index) string {
	quote := db.dialect.Quoter()
	tableName = quote.Quote(tableName)
	indexName := quote.Quote(index.Name)

	colsIndex := make([]string, len(index.Cols))
	for i := 0; i < len(index.Cols); i++ {
		colsIndex[i] = quote.Quote(index.Cols[i])
	}

	indexOn := strings.Join(colsIndex, ",")

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("ALTER TABLE %s ADD INDEX %s GLOBAL ON ( %s );", tableName, indexName, indexOn))

	return buf.String()
}

func (db *ydb) DropIndexSQL(tableName string, index *schemas.Index) string {
	quote := db.dialect.Quoter()
	tableName = quote.Quote(tableName)
	indexName := quote.Quote(index.Name)

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("ALTER TABLE %s DROP INDEX %s;", tableName, indexName))

	return buf.String()
}

func (db *ydb) IsColumnExist(
	queryer core.Queryer,
	ctx context.Context,
	tableName,
	columnName string) (_ bool, err error) {
	var exists bool
	err = db.WithConnRaw(queryer, ctx, func(dc interface{}) error {
		q, ok := dc.(interface {
			IsColumnExists(context.Context, string, string) (bool, error)
		})
		if !ok {
			return fmt.Errorf("driver does not support query metadata")
		}
		exists, err = q.IsColumnExists(ctx, tableName, columnName)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return false, err
	}
	return exists, nil
}

func (db *ydb) GetColumns(queryer core.Queryer, ctx context.Context, tableName string) (
	_ []string,
	_ map[string]*schemas.Column,
	err error) {
	colNames := make([]string, 0)
	colMaps := make(map[string]*schemas.Column)

	err = db.WithConnRaw(queryer, ctx, func(dc interface{}) error {
		q, ok := dc.(interface {
			GetColumns(context.Context, string) ([]string, error)
			GetColumnType(context.Context, string, string) (string, error)
			IsPrimaryKey(context.Context, string, string) (bool, error)
		})
		if !ok {
			return fmt.Errorf("driver does not support query metadata")
		}

		colNames, err = q.GetColumns(ctx, tableName)
		if err != nil {
			return err
		}

		for _, colName := range colNames {
			dataType, err := q.GetColumnType(ctx, tableName, colName)
			if err != nil {
				return err
			}
			dataType = removeOptional(dataType)
			isPK, err := q.IsPrimaryKey(ctx, tableName, colName)
			if err != nil {
				return err
			}
			col := &schemas.Column{
				Name:         colName,
				TableName:    tableName,
				SQLType:      yqlToSQLType(dataType),
				IsPrimaryKey: isPK,
				Nullable:     !isPK,
				Indexes:      make(map[string]int),
			}
			colMaps[colName] = col
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return colNames, colMaps, nil
}

func (db *ydb) GetTables(queryer core.Queryer, ctx context.Context) (_ []*schemas.Table, err error) {
	tables := make([]*schemas.Table, 0)
	err = db.WithConnRaw(queryer, ctx, func(dc interface{}) error {
		q, ok := dc.(interface {
			GetTables(context.Context, string, bool, bool) ([]string, error)
		})
		if !ok {
			return fmt.Errorf("driver does not support query metadata")
		}
		tableNames, err := q.GetTables(ctx, ".", true, true)
		if err != nil {
			return err
		}
		for _, tableName := range tableNames {
			table := schemas.NewEmptyTable()
			table.Name = tableName
			tables = append(tables, table)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tables, nil
}

func (db *ydb) GetIndexes(
	queryer core.Queryer,
	ctx context.Context,
	tableName string) (_ map[string]*schemas.Index, err error) {
	indexes := make(map[string]*schemas.Index, 0)
	err = db.WithConnRaw(queryer, ctx, func(dc interface{}) error {
		q, ok := dc.(interface {
			GetIndexes(context.Context, string) ([]string, error)
			GetIndexColumns(context.Context, string, string) ([]string, error)
		})
		if !ok {
			return fmt.Errorf("driver does not support query metadata")
		}
		indexNames, err := q.GetIndexes(ctx, tableName)
		if err != nil {
			return err
		}
		for _, indexName := range indexNames {
			cols, err := q.GetIndexColumns(ctx, tableName, indexName)
			if err != nil {
				return err
			}
			indexes[indexName] = &schemas.Index{
				Name: indexName,
				Type: schemas.IndexType,
				Cols: cols,
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return indexes, nil
}

// !datbeohbbh! CreateTableSQL generate `CREATE TABLE` YQL.
// Method does not generate YQL for creating index.
func (db *ydb) CreateTableSQL(
	ctx context.Context,
	_ core.Queryer,
	table *schemas.Table,
	tableName string) (string, bool, error) {
	quote := db.dialect.Quoter()
	tableName = quote.Quote(tableName)

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("CREATE TABLE %s ( ", tableName))

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

	buf.WriteString(strings.Join([]string{joinColumns, primaryKey}, ", "))
	buf.WriteString(" ) ")

	if db.tableParams != nil && len(db.tableParams) > 0 {
		params := make([]string, 0)
		for param, value := range db.tableParams {
			if param == "" || value == "" {
				continue
			}
			params = append(params, fmt.Sprintf("%s = %s", param, value))
		}
		if len(params) > 0 {
			buf.WriteString(fmt.Sprintf("WITH ( %s ) ", strings.Join(params, ", ")))
		}
	}

	buf.WriteString("; ")

	return buf.String(), true, nil
}

func (db *ydb) DropTableSQL(tableName string) (string, bool) {
	quote := db.dialect.Quoter()
	tableName = quote.Quote(tableName)

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("DROP TABLE %s;", tableName))

	return buf.String(), false
}

// https://github.com/ydb-platform/ydb-go-sdk/blob/master/SQL.md#specifying-query-parameters-
func (db *ydb) Filters() []Filter {
	return []Filter{&ydbSeqFilter{
		Prefix: "$",
		Start:  1,
	}}
}

const (
	ydb_grpc_Canceled           uint32 = 1
	ydb_grpc_Unknown            uint32 = 2
	ydb_grpc_InvalidArgument    uint32 = 3
	ydb_grpc_DeadlineExceeded   uint32 = 4
	ydb_grpc_NotFound           uint32 = 5
	ydb_grpc_AlreadyExists      uint32 = 6
	ydb_grpc_PermissionDenied   uint32 = 7
	ydb_grpc_ResourceExhausted  uint32 = 8
	ydb_grpc_FailedPrecondition uint32 = 9
	ydb_grpc_Aborted            uint32 = 10
	ydb_grpc_OutOfRange         uint32 = 11
	ydb_grpc_Unimplemented      uint32 = 12
	ydb_grpc_Internal           uint32 = 13
	ydb_grpc_Unavailable        uint32 = 14
	ydb_grpc_DataLoss           uint32 = 15
	ydb_grpc_Unauthenticated    uint32 = 16
)

const (
	ydb_STATUS_CODE_UNSPECIFIED int32 = 0
	ydb_SUCCESS                 int32 = 400000
	ydb_BAD_REQUEST             int32 = 400010
	ydb_UNAUTHORIZED            int32 = 400020
	ydb_INTERNAL_ERROR          int32 = 400030
	ydb_ABORTED                 int32 = 400040
	ydb_UNAVAILABLE             int32 = 400050
	ydb_OVERLOADED              int32 = 400060
	ydb_SCHEME_ERROR            int32 = 400070
	ydb_GENERIC_ERROR           int32 = 400080
	ydb_TIMEOUT                 int32 = 400090
	ydb_BAD_SESSION             int32 = 400100
	ydb_PRECONDITION_FAILED     int32 = 400120
	ydb_ALREADY_EXISTS          int32 = 400130
	ydb_NOT_FOUND               int32 = 400140
	ydb_SESSION_EXPIRED         int32 = 400150
	ydb_CANCELLED               int32 = 400160
	ydb_UNDETERMINED            int32 = 400170
	ydb_UNSUPPORTED             int32 = 400180
	ydb_SESSION_BUSY            int32 = 400190
)

// https://github.com/ydb-platform/ydb-go-sdk/blob/ca13feb3ca560ac7385e79d4365ffe0cd8c23e21/errors.go#L27
func (db *ydb) IsRetryable(err error) bool {
	var target interface {
		error
		Code() int32
		Name() string
	}
	if errors.Is(err, fmt.Errorf("unknown error")) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) {
		return false
	}
	if !errors.As(err, &target) {
		return false
	}

	switch target.Code() {
	case
		int32(ydb_grpc_Unknown),
		int32(ydb_grpc_InvalidArgument),
		int32(ydb_grpc_DeadlineExceeded),
		int32(ydb_grpc_NotFound),
		int32(ydb_grpc_AlreadyExists),
		int32(ydb_grpc_PermissionDenied),
		int32(ydb_grpc_FailedPrecondition),
		int32(ydb_grpc_OutOfRange),
		int32(ydb_grpc_Unimplemented),
		int32(ydb_grpc_DataLoss),
		int32(ydb_grpc_Unauthenticated):
		return false
	case
		int32(ydb_grpc_Canceled),
		int32(ydb_grpc_ResourceExhausted),
		int32(ydb_grpc_Aborted),
		int32(ydb_grpc_Internal),
		int32(ydb_grpc_Unavailable):
		return true
	case
		ydb_STATUS_CODE_UNSPECIFIED,
		ydb_BAD_REQUEST,
		ydb_UNAUTHORIZED,
		ydb_INTERNAL_ERROR,
		ydb_SCHEME_ERROR,
		ydb_GENERIC_ERROR,
		ydb_TIMEOUT,
		ydb_PRECONDITION_FAILED,
		ydb_ALREADY_EXISTS,
		ydb_NOT_FOUND,
		ydb_SESSION_EXPIRED,
		ydb_CANCELLED,
		ydb_UNSUPPORTED:
		return false
	case
		ydb_ABORTED,
		ydb_UNAVAILABLE,
		ydb_OVERLOADED,
		ydb_BAD_SESSION,
		ydb_UNDETERMINED,
		ydb_SESSION_BUSY:
		return true
	default:
		return false
	}
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

// https://pkg.go.dev/database/sql#ColumnType.DatabaseTypeName
func (ydbDrv *ydbDriver) GenScanResult(columnType string) (interface{}, error) {
	switch columnType = removeOptional(columnType); columnType {
	case yql_Bool:
		var ret sql.NullBool
		return &ret, nil
	case yql_Int16:
		var ret sql.NullInt16
		return &ret, nil
	case yql_Int32:
		var ret sql.NullInt32
		return &ret, nil
	case yql_Int64:
		var ret sql.NullInt64
		return &ret, nil
	case yql_Uint8:
		var ret sql.NullByte
		return &ret, nil
	case yql_Uint32:
		var ret convert.NullUint32
		return &ret, nil
	case yql_Uint64:
		var ret convert.NullUint64
		return &ret, nil
	case yql_Double:
		var ret sql.NullFloat64
		return &ret, nil
	case yql_Utf8:
		var ret sql.NullString
		return &ret, nil
	case yql_Timestamp:
		var ret sql.NullTime
		return &ret, nil
	case yql_Interval:
		var ret convert.NullDuration
		return &ret, nil
	default:
		var ret sql.RawBytes
		return &ret, nil
	}
}

func (ydbDrv *ydbDriver) Scan(ctx *ScanContext, rows *core.Rows, types []*sql.ColumnType, v ...interface{}) error {
	if err := rows.Scan(v...); err != nil {
		return err
	}

	if ctx.UserLocation == nil {
		return nil
	}

	for i := range v {
		// !datbeohbbh! YDB saves time in UTC. When returned value is time type, then value will be represented in local time.
		// So value in time type must be converted to UserLocation.
		switch des := v[i].(type) {
		case *time.Time:
			*des = (*des).In(ctx.UserLocation)
		case *sql.NullTime:
			if des.Valid {
				(*des).Time = (*des).Time.In(ctx.UserLocation)
			}
		case *interface{}:
			switch t := (*des).(type) {
			case time.Time:
				*des = t.In(ctx.UserLocation)
			case sql.NullTime:
				if t.Valid {
					*des = t.Time.In(ctx.UserLocation)
				}
			}
		}
	}

	return nil
}
