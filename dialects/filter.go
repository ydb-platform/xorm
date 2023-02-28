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

// generate `DECLARE` section
// https://github.com/ydb-platform/ydb-go-sdk/blob/master/SQL.md#specifying-query-parameters-
func (yf *SeqFilter) GenerateDeclareSection(args ...interface{}) string {
	if len(args) == 0 {
		return ""
	}

	var (
		index      = yf.Start
		declareBuf strings.Builder
	)

	const (
		declareOptional    string = "DECLARE %s AS OPTIONAL<%s>;"
		declareNonOptional string = "DECLARE %s AS %s;"
	)

	for _, arg := range args {
		var t sql.NamedArg

		if c, ok := arg.(sql.NamedArg); ok {
			t.Name = fmt.Sprintf("%s%v", strings.TrimPrefix(yf.Prefix, "$"), index)
			t.Value = GetActualValue(reflect.ValueOf(c.Value))
		} else {
			t = sql.Named(
				fmt.Sprintf("%s%v", strings.TrimPrefix(yf.Prefix, "$"), index),
				GetActualValue(reflect.ValueOf(arg)),
			)
		}
		args[index-1] = t

		var (
			st          schemas.SQLType
			tp          string
			declareType string
		)

		// !datbeohbbh! if can not infer the type. tp = "Optional<String>"
		if reflect.ValueOf(t.Value).Kind() == reflect.Invalid {
			declareType = declareOptional
			tp = yql_String
		} else {
			if reflect.ValueOf(t.Value).Kind() == reflect.Ptr && reflect.ValueOf(t.Value).IsNil() {
				declareType = declareOptional
			} else {
				declareType = declareNonOptional
			}
			st = schemas.Type2SQLType2(reflect.TypeOf(t.Value))
			tp = toYQLDataType(st.Name, st.DefaultLength, st.DefaultLength2)

			switch tp {
			case yql_String:
				if reflect.TypeOf(t.Value).Kind() == reflect.Struct {
					fields := make([]string, 0)
					to := reflect.TypeOf(t.Value)
					for i := 0; i < to.NumField(); i++ {
						st = schemas.Type2SQLType2(to.Field(i).Type)
						tElem := toYQLDataType(st.Name, st.DefaultLength, st.DefaultLength2)
						fields = append(fields, fmt.Sprintf("%s:%s", to.Field(i).Name, tElem))
					}
					tp = fmt.Sprintf("Struct<%s>", strings.Join(fields, ","))
				}
			case yql_List:
				st = schemas.Type2SQLType2(reflect.TypeOf(t.Value).Elem())
				tElem := toYQLDataType(st.Name, st.DefaultLength, st.DefaultLength2)
				tp = fmt.Sprintf("List<%s>", tElem)
			}
		}

		declareBuf.WriteString(fmt.Sprintf(declareType, fmt.Sprintf("%s%v", yf.Prefix, index), tp))

		index++
	}

	return declareBuf.String()
}
