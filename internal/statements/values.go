// Copyright 2017 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package statements

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/big"
	"reflect"
	"time"

	"xorm.io/xorm/convert"
	"xorm.io/xorm/dialects"
	"xorm.io/xorm/internal/json"
	"xorm.io/xorm/schemas"
)

var (
	nullFloatType = reflect.TypeOf(sql.NullFloat64{})
	bigFloatType  = reflect.TypeOf(big.Float{})
)

// Value2Interface convert a field value of a struct to interface for putting into database
func (statement *Statement) Value2Interface(col *schemas.Column, fieldValue reflect.Value) (interface{}, error) {
	if fieldValue.CanAddr() {
		if fieldConvert, ok := fieldValue.Addr().Interface().(convert.Conversion); ok {
			data, err := fieldConvert.ToDB()
			if err != nil {
				return nil, err
			}
			if data == nil {
				if col.Nullable {
					return nil, nil
				}
				data = []byte{}
			}
			if col.SQLType.IsBlob() {
				return data, nil
			}
			return string(data), nil
		}
	}

	isNil := fieldValue.Kind() == reflect.Ptr && fieldValue.IsNil()
	if !isNil {
		if fieldConvert, ok := fieldValue.Interface().(convert.Conversion); ok {
			data, err := fieldConvert.ToDB()
			if err != nil {
				return nil, err
			}
			if data == nil {
				if col.Nullable {
					return nil, nil
				}
				data = []byte{}
			}
			if col.SQLType.IsBlob() {
				return data, nil
			}
			return string(data), nil
		}
	}

	fieldType := fieldValue.Type()
	k := fieldType.Kind()
	if k == reflect.Ptr {
		if fieldValue.IsNil() {
			return nil, nil
		} else if !fieldValue.IsValid() {
			return nil, nil
		} else {
			// !nashtsai! deference pointer type to instance type
			fieldValue = fieldValue.Elem()
			fieldType = fieldValue.Type()
			k = fieldType.Kind()
		}
	}

	switch k {
	case reflect.Bool:
		return fieldValue.Bool(), nil
	case reflect.String:
		return fieldValue.String(), nil
	case reflect.Struct:
		if fieldType.ConvertibleTo(schemas.TimeType) {
			t := fieldValue.Convert(schemas.TimeType).Interface().(time.Time)
			tf, err := dialects.FormatColumnTime(statement.dialect, statement.defaultTimeZone, col, t)
			return tf, err
		} else if fieldType.ConvertibleTo(nullFloatType) {
			t := fieldValue.Convert(nullFloatType).Interface().(sql.NullFloat64)
			if !t.Valid {
				return nil, nil
			}
			return t.Float64, nil
		} else if fieldType.ConvertibleTo(bigFloatType) {
			t := fieldValue.Convert(bigFloatType).Interface().(big.Float)
			return t.String(), nil
		}

		if !col.IsJSON {
			// !<winxxp>! 增加支持driver.Valuer接口的结构，如sql.NullString
			if v, ok := fieldValue.Interface().(driver.Valuer); ok {
				return v.Value()
			}

			fieldTable, err := statement.tagParser.ParseWithCache(fieldValue)
			if err != nil {
				return nil, err
			}
			if len(fieldTable.PrimaryKeys) == 1 {
				pkField := reflect.Indirect(fieldValue).FieldByName(fieldTable.PKColumns()[0].FieldName)
				return pkField.Interface(), nil
			}
			return nil, fmt.Errorf("no primary key for col %v", col.Name)
		}

		if col.SQLType.IsText() {
			bytes, err := json.DefaultJSONHandler.Marshal(fieldValue.Interface())
			if err != nil {
				return nil, err
			}
			return string(bytes), nil
		} else if col.SQLType.IsBlob() {
			bytes, err := json.DefaultJSONHandler.Marshal(fieldValue.Interface())
			if err != nil {
				return nil, err
			}
			return bytes, nil
		}
		return nil, fmt.Errorf("Unsupported type %v", fieldValue.Type())
	case reflect.Complex64, reflect.Complex128:
		bytes, err := json.DefaultJSONHandler.Marshal(fieldValue.Interface())
		if err != nil {
			return nil, err
		}
		return string(bytes), nil
	case reflect.Array, reflect.Slice, reflect.Map:
		if !fieldValue.IsValid() {
			return fieldValue.Interface(), nil
		}

		if col.SQLType.IsText() {
			bytes, err := json.DefaultJSONHandler.Marshal(fieldValue.Interface())
			if err != nil {
				return nil, err
			}
			return string(bytes), nil
		} else if col.SQLType.IsBlob() {
			var bytes []byte
			var err error
			if (k == reflect.Slice) &&
				(fieldValue.Type().Elem().Kind() == reflect.Uint8) {
				bytes = fieldValue.Bytes()
			} else {
				bytes, err = json.DefaultJSONHandler.Marshal(fieldValue.Interface())
				if err != nil {
					return nil, err
				}
			}
			return bytes, nil
		}
		return nil, ErrUnSupportedType
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		return fieldValue.Uint(), nil
	default:
		return fieldValue.Interface(), nil
	}
}

func (statement *Statement) YQL_ValueToInterface(col *schemas.Column, fieldValue reflect.Value) (interface{}, error) {
	// @TODO: handle Conversion type
	fieldType := fieldValue.Type()
	k := fieldType.Kind()
	if k == reflect.Ptr {
		if fieldValue.IsNil() {
			return nil, nil
		} else if !fieldValue.IsValid() {
			return nil, nil
		} else {
			fieldValue = fieldValue.Elem()
			fieldType = fieldValue.Type()
			k = fieldType.Kind()
		}
	}

	switch k {
	case reflect.Bool:
		return fieldValue.Bool(), nil
	case reflect.String:
		return fieldValue.String(), nil
	case reflect.Struct:
		if fieldType.ConvertibleTo(schemas.TimeType) {
			t := fieldValue.Convert(schemas.TimeType).Interface().(time.Time)
			tf, err := dialects.FormatColumnTime(statement.dialect, statement.defaultTimeZone, col, t)
			return tf, err
		} else if fieldType.ConvertibleTo(schemas.IntervalType) {
			t := fieldValue.Convert(schemas.IntervalType).Interface().(time.Duration)
			return t, nil
		} else if fieldType.ConvertibleTo(schemas.NullBoolType) {
			t := fieldValue.Convert(schemas.NullBoolType).Interface().(sql.NullBool)
			if !t.Valid {
				return nil, nil
			}
			return t.Bool, nil
		} else if fieldType.ConvertibleTo(schemas.NullFloat64Type) {
			t := fieldValue.Convert(schemas.NullFloat64Type).Interface().(sql.NullFloat64)
			if !t.Valid {
				return nil, nil
			}
			return t.Float64, nil
		} else if fieldType.ConvertibleTo(schemas.NullInt16Type) {
			t := fieldValue.Convert(schemas.NullInt16Type).Interface().(sql.NullInt16)
			if !t.Valid {
				return nil, nil
			}
			return t.Int16, nil
		} else if fieldType.ConvertibleTo(schemas.NullInt32Type) {
			t := fieldValue.Convert(schemas.NullInt32Type).Interface().(sql.NullInt32)
			if !t.Valid {
				return nil, nil
			}
			return t.Int32, nil
		} else if fieldType.ConvertibleTo(schemas.NullInt64Type) {
			t := fieldValue.Convert(schemas.NullInt64Type).Interface().(sql.NullInt64)
			if !t.Valid {
				return nil, nil
			}
			return t.Int64, nil
		} else if fieldType.ConvertibleTo(schemas.NullStringType) {
			t := fieldValue.Convert(schemas.NullStringType).Interface().(sql.NullString)
			if !t.Valid {
				return nil, nil
			}
			return t.String, nil
		} else if fieldType.ConvertibleTo(schemas.NullTimeType) {
			t := fieldValue.Convert(schemas.NullTimeType).Interface().(sql.NullTime)
			if !t.Valid {
				return nil, nil
			}
			return t.Time, nil
		}

		if col.SQLType.IsBlob() {
			bytes, err := json.DefaultJSONHandler.Marshal(fieldValue.Interface())
			if err != nil {
				return nil, err
			}
			return bytes, nil
		}

		return nil, ErrUnSupportedType
	case reflect.Array, reflect.Slice:
		if !fieldValue.IsValid() {
			return fieldValue.Interface(), nil
		}

		if col.SQLType.IsBlob() {
			return fieldValue.Bytes(), nil
		} else if col.SQLType.IsArray() {
			return fieldValue.Interface(), nil
		}
		return nil, ErrUnSupportedType
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		val := fieldValue.Uint()
		switch t := col.SQLType.Name; t {
		case schemas.UnsignedTinyInt:
			return uint8(val), nil
		case schemas.TinyInt:
			return int8(val), nil
		case schemas.UnsignedSmallInt:
			return uint16(val), nil
		case schemas.SmallInt:
			return int16(val), nil
		case schemas.UnsignedMediumInt, schemas.UnsignedInt:
			return uint32(val), nil
		case schemas.MediumInt, schemas.Int:
			return int32(val), nil
		case schemas.UnsignedBigInt:
			return uint64(val), nil
		case schemas.BigInt:
			return int64(val), nil
		default:
			return val, nil
		}
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		val := fieldValue.Int()
		switch t := col.SQLType.Name; t {
		case schemas.TinyInt:
			return int8(val), nil
		case schemas.SmallInt:
			return int16(val), nil
		case schemas.MediumInt, schemas.Int:
			return int32(val), nil
		case schemas.BigInt:
			return int64(val), nil
		default:
			return val, nil
		}
	default:
		return fieldValue.Interface(), nil
	}
}
