// Copyright 2019 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dialects

import (
	"fmt"
	"reflect"
	"strings"

	"database/sql"

	"xorm.io/xorm/schemas"
)

// Filter is an interface to filter SQL
type Filter interface {
	Do(sql string) string
	DoWithDeclare(sqlStr string, args ...interface{}) string
}

// SeqFilter filter SQL replace ?, ? ... to $1, $2 ...
type SeqFilter struct {
	Prefix string
	Start  int
}

func convertQuestionMark(sql, prefix string, start int) string {
	var buf strings.Builder
	var beginSingleQuote bool
	var isLineComment bool
	var isComment bool
	var isMaybeLineComment bool
	var isMaybeComment bool
	var isMaybeCommentEnd bool
	var index = start
	for _, c := range sql {
		if !beginSingleQuote && !isLineComment && !isComment && c == '?' {
			buf.WriteString(fmt.Sprintf("%s%v", prefix, index))
			index++
		} else {
			if isMaybeLineComment {
				if c == '-' {
					isLineComment = true
				}
				isMaybeLineComment = false
			} else if isMaybeComment {
				if c == '*' {
					isComment = true
				}
				isMaybeComment = false
			} else if isMaybeCommentEnd {
				if c == '/' {
					isComment = false
				}
				isMaybeCommentEnd = false
			} else if isLineComment {
				if c == '\n' {
					isLineComment = false
				}
			} else if isComment {
				if c == '*' {
					isMaybeCommentEnd = true
				}
			} else if !beginSingleQuote && c == '-' {
				isMaybeLineComment = true
			} else if !beginSingleQuote && c == '/' {
				isMaybeComment = true
			} else if c == '\'' {
				beginSingleQuote = !beginSingleQuote
			}
			buf.WriteRune(c)
		}
	}
	return buf.String()
}

// Do implements Filter
func (s *SeqFilter) Do(sql string) string {
	return convertQuestionMark(sql, s.Prefix, s.Start)
}

// NEDD TEST
// mapping ?, ? ... -> $ydb_placeholer_1, $ydb_placeholder_2 ...
// https://github.com/ydb-platform/ydb-go-sdk/blob/master/SQL.md#specifying-query-parameters-
func (yf *SeqFilter) DoWithDeclare(sqlStr string, args ...interface{}) string {
	var buf strings.Builder
	var declareBuf strings.Builder

	var beginSingleQuote bool
	var isLineComment bool
	var isComment bool
	var isMaybeLineComment bool
	var isMaybeComment bool
	var isMaybeCommentEnd bool

	var index = yf.Start

	// log.Println(sqlStr, reflect.ValueOf(args))
	for _, c := range sqlStr {
		if !beginSingleQuote && !isLineComment && !isComment && c == '?' {
			// t, ok := reflect.ValueOf(args[index-1]).Interface().(sql.NamedArg)
			t, ok := args[index-1].(sql.NamedArg)
			if !ok {
				panic(fmt.Errorf("args should be the `sql.NamedArg` type: %+v", args[index-1]))
			}

			repl := fmt.Sprintf("%s%s", yf.Prefix, t.Name)
			st := schemas.Type2SQLType(reflect.TypeOf(t.Value))
			tp := toYQLDataType(st.Name, st.DefaultLength, st.DefaultLength2)

			declareBuf.WriteString(fmt.Sprintf("DECLARE %s AS %s;", repl, tp))

			buf.WriteString(repl)
			index++
		} else {
			if isMaybeLineComment {
				if c == '-' {
					isLineComment = true
				}
				isMaybeLineComment = false
			} else if isMaybeComment {
				if c == '*' {
					isComment = true
				}
				isMaybeComment = false
			} else if isMaybeCommentEnd {
				if c == '/' {
					isComment = false
				}
				isMaybeCommentEnd = false
			} else if isLineComment {
				if c == '\n' {
					isLineComment = false
				}
			} else if isComment {
				if c == '*' {
					isMaybeCommentEnd = true
				}
			} else if !beginSingleQuote && c == '-' {
				isMaybeLineComment = true
			} else if !beginSingleQuote && c == '/' {
				isMaybeComment = true
			} else if c == '\'' {
				beginSingleQuote = !beginSingleQuote
			}
			buf.WriteRune(c)
		}
	}
	
	return declareBuf.String() + buf.String()
}
