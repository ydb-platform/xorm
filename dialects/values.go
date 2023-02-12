package dialects

import (
	"database/sql"
	"reflect"
	"time"

	"xorm.io/xorm/schemas"
)

// !datbeohbbh! this is a 'helper' function for YDB to generate declare section
func getActualValue(fieldValue reflect.Value) interface{} {
	fieldType := fieldValue.Type()
	k := fieldType.Kind()
	if k == reflect.Ptr {
		if fieldValue.IsNil() || !fieldValue.IsValid() {
			return fieldValue.Interface()
		} else {
			fieldValue = fieldValue.Elem()
			fieldType = fieldValue.Type()
			k = fieldType.Kind()
		}
	}

	switch k {
	case reflect.Bool:
		return fieldValue.Bool()
	case reflect.String:
		return fieldValue.String()
	case reflect.Struct:
		if fieldType.ConvertibleTo(schemas.TimeType) {
			t := fieldValue.Convert(schemas.TimeType).Interface().(time.Time)
			return t
		} else if fieldType.ConvertibleTo(schemas.IntervalType) {
			t := fieldValue.Convert(schemas.IntervalType).Interface().(time.Duration)
			return t
		} else if fieldType.ConvertibleTo(schemas.NullBoolType) {
			t := fieldValue.Convert(schemas.NullBoolType).Interface().(sql.NullBool)
			if !t.Valid {
				var ret *bool
				return ret
			}
			return t.Bool
		} else if fieldType.ConvertibleTo(schemas.NullFloat64Type) {
			t := fieldValue.Convert(schemas.NullFloat64Type).Interface().(sql.NullFloat64)
			if !t.Valid {
				var ret *float64
				return ret
			}
			return t.Float64
		} else if fieldType.ConvertibleTo(schemas.NullInt16Type) {
			t := fieldValue.Convert(schemas.NullInt16Type).Interface().(sql.NullInt16)
			if !t.Valid {
				var ret *int64
				return ret
			}
			return t.Int16
		} else if fieldType.ConvertibleTo(schemas.NullInt32Type) {
			t := fieldValue.Convert(schemas.NullInt32Type).Interface().(sql.NullInt32)
			if !t.Valid {
				var ret *int32
				return ret
			}
			return t.Int32
		} else if fieldType.ConvertibleTo(schemas.NullInt64Type) {
			t := fieldValue.Convert(schemas.NullInt64Type).Interface().(sql.NullInt64)
			if !t.Valid {
				var ret *int64
				return ret
			}
			return t.Int64
		} else if fieldType.ConvertibleTo(schemas.NullStringType) {
			t := fieldValue.Convert(schemas.NullStringType).Interface().(sql.NullString)
			if !t.Valid {
				var ret *string
				return ret
			}
			return t.String
		} else if fieldType.ConvertibleTo(schemas.NullTimeType) {
			t := fieldValue.Convert(schemas.NullTimeType).Interface().(sql.NullTime)
			if !t.Valid {
				var ret *time.Time
				return ret
			}
			return t.Time
		}
		return fieldValue.Interface()
	case reflect.Array, reflect.Slice, reflect.Map:
		if !fieldValue.IsValid() {
			return fieldValue.Interface()
		}
		return fieldValue.Interface()
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		val := fieldValue.Uint()
		switch k {
		case reflect.Uint8:
			return uint8(val)
		case reflect.Uint16:
			return uint16(val)
		case reflect.Uint32:
			return uint32(val)
		case reflect.Uint64:
			return uint64(val)
		default:
			return uint32(val)
		}
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		val := fieldValue.Int()
		switch k {
		case reflect.Int8:
			return int8(val)
		case reflect.Int16:
			return int16(val)
		case reflect.Int32:
			return int32(val)
		case reflect.Int64:
			return int64(val)
		default:
			return int32(val)
		}
	default:
		if fieldValue.Interface() == nil {
			var ret *[]byte
			return ret
		}
		return fieldValue.Interface()
	}
}
