package dialects

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

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
	case schemas.Varchar, schemas.Text:
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
		sqlType = schemas.SQLType{schemas.Blob, 0, 0}
	}
	return
}

func removeOptional(s string) string {
	if strings.HasPrefix(s, "Optional") {
		s = strings.TrimPrefix(s, "Optional<")
		s = strings.TrimSuffix(s, ">")
	}
	return s
}

type ydb struct {
	Base
	ydb *core.DB
}

func (db *ydb) Init(uri *URI) error {
	db.quoter = ydbQuoter
	return db.Base.Init(db, uri)
}

// !datbeohbbh! set the internal *core.DB, this function is necessary for YDB
// because to query the metadata in YDB, need to provide *sql.Conn
func (db *ydb) SetInternal(initDB *core.DB) {
	db.ydb = initDB
}

func (db *ydb) getDB() *core.DB {
	return db.ydb
}

func (db *ydb) WithConn(ctx context.Context, f func(context.Context, *sql.Conn) error) error {
	cc, err := db.getDB().Conn(ctx)
	if err != nil {
		return err
	}
	defer cc.Close()

	err = f(ctx, cc)

	return err
}

func (db *ydb) WithConnRaw(ctx context.Context, f func(d interface{}) error) (err error) {
	err = db.WithConn(ctx, func(ctx context.Context, cc *sql.Conn) error {
		err = cc.Raw(f)
		return err
	})
	return err
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

func (db *ydb) Version(ctx context.Context, _ core.Queryer) (_ *schemas.Version, err error) {
	var version string
	err = db.WithConnRaw(ctx, func(dc interface{}) error {
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
	_ core.Queryer,
	ctx context.Context,
	tableName string) (_ bool, err error) {
	var exists bool
	err = db.WithConnRaw(ctx, func(dc interface{}) error {
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
	_ core.Queryer,
	ctx context.Context,
	tableName,
	columnName string) (_ bool, err error) {
	var exists bool
	err = db.WithConnRaw(ctx, func(dc interface{}) error {
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

func (db *ydb) GetColumns(_ core.Queryer, ctx context.Context, tableName string) (
	_ []string,
	_ map[string]*schemas.Column,
	err error) {
	colNames := make([]string, 0)
	colMaps := make(map[string]*schemas.Column)

	err = db.WithConnRaw(ctx, func(dc interface{}) error {
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

func (db *ydb) GetTables(_ core.Queryer, ctx context.Context) (_ []*schemas.Table, err error) {
	tables := make([]*schemas.Table, 0)
	err = db.WithConnRaw(ctx, func(dc interface{}) error {
		q, ok := dc.(interface {
			GetAllTables(context.Context, string) ([]string, error)
		})
		if !ok {
			return fmt.Errorf("driver does not support query metadata")
		}
		tableNames, err := q.GetAllTables(ctx, ".")
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
	_ core.Queryer,
	ctx context.Context,
	tableName string) (_ map[string]*schemas.Index, err error) {
	indexes := make(map[string]*schemas.Index, 0)
	err = db.WithConnRaw(ctx, func(dc interface{}) error {
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
	tableName = quote.Quote(tableName)

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("DROP TABLE %s;", tableName))

	return buf.String(), false
}

// https://github.com/ydb-platform/ydb-go-sdk/blob/master/SQL.md#specifying-query-parameters-
func (db *ydb) Filters() []Filter {
	return []Filter{&SeqFilter{
		Prefix: "$",
		Start:  1,
	}}
}

const (
	grpc_Canceled           uint32 = 1
	grpc_Unknown            uint32 = 2
	grpc_InvalidArgument    uint32 = 3
	grpc_DeadlineExceeded   uint32 = 4
	grpc_NotFound           uint32 = 5
	grpc_AlreadyExists      uint32 = 6
	grpc_PermissionDenied   uint32 = 7
	grpc_ResourceExhausted  uint32 = 8
	grpc_FailedPrecondition uint32 = 9
	grpc_Aborted            uint32 = 10
	grpc_OutOfRange         uint32 = 11
	grpc_Unimplemented      uint32 = 12
	grpc_Internal           uint32 = 13
	grpc_Unavailable        uint32 = 14
	grpc_DataLoss           uint32 = 15
	grpc_Unauthenticated    uint32 = 16
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
		int32(grpc_Unknown),
		int32(grpc_InvalidArgument),
		int32(grpc_DeadlineExceeded),
		int32(grpc_NotFound),
		int32(grpc_AlreadyExists),
		int32(grpc_PermissionDenied),
		int32(grpc_FailedPrecondition),
		int32(grpc_OutOfRange),
		int32(grpc_Unimplemented),
		int32(grpc_DataLoss),
		int32(grpc_Unauthenticated):
		return false
	case
		int32(grpc_Canceled),
		int32(grpc_ResourceExhausted),
		int32(grpc_Aborted),
		int32(grpc_Internal),
		int32(grpc_Unavailable):
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

// https://pkg.go.dev/database/sql#ColumnType.DatabaseTypeName
func (ydbDrv *ydbDriver) GenScanResult(columnType string) (interface{}, error) {
	columnType = strings.ToUpper(removeOptional(columnType))
	switch columnType {
	case "BOOL":
		var ret sql.NullBool
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
	case "UINT32":
		var ret convert.NullUint32
		return &ret, nil
	case "UINT64":
		var ret convert.NullUint64
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
