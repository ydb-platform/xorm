package dialects

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseYDBConnString(t *testing.T) {
	type result struct {
		dbType   string
		host     string
		port     string
		dbName   string
		userName string
		password string
	}

	tests := []struct {
		connString string
		expected   result
		valid      bool
	}{
		{
			connString: "grpc://localhost:2136/local",
			expected: result{
				dbType:   "ydb",
				host:     "localhost",
				port:     "2136",
				dbName:   "/local",
				userName: "",
				password: "",
			},
			valid: true,
		},
		{
			connString: "grpcs://localhost:2135/local",
			expected: result{
				dbType:   "ydb",
				host:     "localhost",
				port:     "2135",
				dbName:   "/local",
				userName: "",
				password: "",
			},
			valid: true,
		},
		{
			connString: "grpcs://ydb.serverless.yandexcloud.net:2135/ru-central1/b1g8skpblkos03malf3s/etn01q5ko6sh271beftr",
			expected: result{
				dbType:   "ydb",
				host:     "ydb.serverless.yandexcloud.net",
				port:     "2135",
				dbName:   "/ru-central1/b1g8skpblkos03malf3s/etn01q5ko6sh271beftr",
				userName: "",
				password: "",
			},
			valid: true,
		},
		{
			connString: "https://localhost:2135/local",
			expected:   result{},
			valid:      false,
		},
		{
			connString: "grpcs://localhost:2135/local?query_mode=data&go_query_bind=table_path_prefix(/local/test),numeric,declare",
			expected: result{
				dbType:   "ydb",
				host:     "localhost",
				port:     "2135",
				dbName:   "/local",
				userName: "",
				password: "",
			},
			valid: true,
		},
		{
			connString: "grpcs://user:password@localhost:2135/local",
			expected: result{
				dbType:   "ydb",
				host:     "localhost",
				port:     "2135",
				dbName:   "/local",
				userName: "user",
				password: "password",
			},
			valid: true,
		},
	}

	driver := QueryDriver("ydb")
	for _, test := range tests {
		t.Run(test.connString, func(t *testing.T) {
			info, err := driver.Parse("ydb", test.connString)

			if err != nil && test.valid {
				t.Errorf("%q got unexpected error: %s", test.connString, err)
			} else if err == nil {
				expected := test.expected
				actual := result{}
				if test.valid {
					actual = result{
						dbType:   string(info.DBType),
						host:     info.Host,
						port:     info.Port,
						dbName:   info.DBName,
						userName: info.User,
						password: info.Passwd,
					}
				}
				if !reflect.DeepEqual(expected, actual) {
					t.Errorf("%q got: %+v want: %+v", test.connString, actual, expected)
				}
			}
		})
	}
}

// error object for testing `IsRetryable()` method of YDB.
type mockError struct {
	code int32
	name string
}

func (merr mockError) Error() string {
	return fmt.Sprintf("%d/%s", merr.code, merr.name)
}

func (merr mockError) Code() int32 {
	return merr.code
}

func (merr mockError) Name() string {
	return merr.name
}

func TestIsRetryableYDB(t *testing.T) {
	ydbDialect := QueryDialect("ydb") // get ydb dialect

	for _, curErr := range []struct {
		retryable bool
		err       error
	}{
		{
			retryable: false,
			err:       fmt.Errorf("unknown error"),
		},
		{
			retryable: false,
			err:       fmt.Errorf("errors.As() failed"),
		},
		{
			retryable: false,
			err:       context.DeadlineExceeded,
		},
		{
			retryable: false,
			err:       context.Canceled,
		},
		{
			retryable: false,
			err: mockError{
				code: int32(grpc_Unknown),
				name: "grpc unknown",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: int32(grpc_InvalidArgument),
				name: "grpc invalid argument",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: int32(grpc_DeadlineExceeded),
				name: "grpc deadline exceeded",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: int32(grpc_NotFound),
				name: "grpc not found",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: int32(grpc_AlreadyExists),
				name: "grpc already exists",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: int32(grpc_PermissionDenied),
				name: "grpc permission denied",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: int32(grpc_FailedPrecondition),
				name: "grpc failed precondition",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: int32(grpc_OutOfRange),
				name: "grpc out of range",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: int32(grpc_Unimplemented),
				name: "grpc unimplemented",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: int32(grpc_DataLoss),
				name: "grpc data loss",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: int32(grpc_Unauthenticated),
				name: "grpc unauthenticated",
			},
		},
		{
			retryable: true,
			err: mockError{
				code: int32(grpc_Canceled),
				name: "grpc canceled",
			},
		},
		{
			retryable: true,
			err: mockError{
				code: int32(grpc_ResourceExhausted),
				name: "grpc resource exhauseed",
			},
		},
		{
			retryable: true,
			err: mockError{
				code: int32(grpc_Aborted),
				name: "grpc aborted",
			},
		},
		{
			retryable: true,
			err: mockError{
				code: int32(grpc_Internal),
				name: "grpc internal",
			},
		},
		{
			retryable: true,
			err: mockError{
				code: int32(grpc_Unavailable),
				name: "grpc unavailable",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: ydb_STATUS_CODE_UNSPECIFIED,
				name: "ydb status code unspecified",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: ydb_BAD_REQUEST,
				name: "ydb bad request",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: ydb_UNAUTHORIZED,
				name: "ydb unauthorized",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: ydb_INTERNAL_ERROR,
				name: "ydb internal error",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: ydb_SCHEME_ERROR,
				name: "ydb scheme error",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: ydb_GENERIC_ERROR,
				name: "ydb generic error",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: ydb_TIMEOUT,
				name: "ydb timeout",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: ydb_PRECONDITION_FAILED,
				name: "ydb precondition failed",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: ydb_ALREADY_EXISTS,
				name: "ydb already exists",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: ydb_NOT_FOUND,
				name: "ydb not found",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: ydb_SESSION_EXPIRED,
				name: "ydb session expired",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: ydb_CANCELLED,
				name: "ydb cancelled",
			},
		},
		{
			retryable: false,
			err: mockError{
				code: ydb_UNSUPPORTED,
				name: "ydb unsupported",
			},
		},
		{
			retryable: true,
			err: mockError{
				code: ydb_ABORTED,
				name: "ydb aborted",
			},
		},
		{
			retryable: true,
			err: mockError{
				code: ydb_UNAVAILABLE,
				name: "ydb unavailable",
			},
		},
		{
			retryable: true,
			err: mockError{
				code: ydb_OVERLOADED,
				name: "ydb overloaded",
			},
		},
		{
			retryable: true,
			err: mockError{
				code: ydb_BAD_SESSION,
				name: "ydb bad session",
			},
		},
		{
			retryable: true,
			err: mockError{
				code: ydb_UNDETERMINED,
				name: "ydb undetermined",
			},
		},
		{
			retryable: true,
			err: mockError{
				code: ydb_SESSION_BUSY,
				name: "ydb session busy",
			},
		},
		{
			retryable: false,
			err:       fmt.Errorf("wrap error: %w", mockError{code: int32(grpc_Unknown), name: "wrap grpc unknown"}),
		},
		{
			retryable: true,
			err:       fmt.Errorf("wrap error: %w", mockError{code: int32(ydb_UNAVAILABLE), name: "wrap ydb unavailable"}),
		},
		{
			retryable: false,
			err:       fmt.Errorf("wrap error: %w", mockError{code: -1, name: "unknown error"}),
		},
	} {
		t.Run(curErr.err.Error(), func(t *testing.T) {
			retryable := ydbDialect.IsRetryable(curErr.err)
			assert.EqualValues(t, curErr.retryable, retryable, fmt.Errorf("expected: %s - retryable: %v", curErr.err.Error(), curErr.retryable))
		})
	}
}
