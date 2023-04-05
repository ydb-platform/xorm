package dialects

import (
	"reflect"
	"testing"
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
